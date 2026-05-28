"""
Stock screener with persistent caching and daily price refresh.

Data layers:
- Fundamentals (quarterly/annual): cached 30 days per ticker, from Yahoo + FMP fallback
- Price data (market cap): refreshed daily from Yahoo screener bulk quotes
- Screening: combines cached fundamentals + fresh price for daily-accurate P/FCF

Pipeline:
1. Yahoo screener (server-side): pre-filter to ~400 candidates
2. Load cached fundamentals, identify missing tickers
3. Fetch missing from Yahoo per-ticker (bulk of work, but only once per 30 days)
4. Lazy-load from FMP for persistent Yahoo failures (respects daily budget)
5. Combine fundamentals + fresh market cap → apply all filters
"""

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import requests
import yfinance as yf
from yfinance.screener import EquityQuery, screen

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.environ.get("SCREENER_CACHE_DIR", Path(__file__).parent / "cache"))
CACHE_DIR.mkdir(exist_ok=True)
FUNDAMENTALS_DIR = CACHE_DIR / "fundamentals"
FUNDAMENTALS_DIR.mkdir(exist_ok=True)

FUNDAMENTALS_MAX_AGE_DAYS = int(os.environ.get("FUNDAMENTALS_MAX_AGE_DAYS", "30"))
FMP_FAILURE_CACHE_DAYS = int(os.environ.get("FMP_FAILURE_CACHE_DAYS", "30"))
FMP_DAILY_BUDGET = int(os.environ.get("FMP_DAILY_BUDGET", "250"))
FMP_MAX_TICKERS_PER_RUN = int(os.environ.get("FMP_MAX_TICKERS_PER_RUN", "20"))
FMP_MIN_MARKET_CAP = int(os.environ.get("FMP_MIN_MARKET_CAP", "5000000000"))
FMP_MIN_SUCCESS_RATE = float(os.environ.get("FMP_MIN_SUCCESS_RATE", "0.10"))
FMP_MIN_ATTEMPTS_BEFORE_STOP = int(os.environ.get("FMP_MIN_ATTEMPTS_BEFORE_STOP", "10"))
DATA_STALE_HOURS = float(os.environ.get("DATA_STALE_HOURS", "84"))
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"

RUN_STATS_FILE = CACHE_DIR / "run_stats.json"
LAST_ERROR_FILE = CACHE_DIR / "last_error.json"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utcnow().isoformat().replace("+00:00", "Z")


def _format_mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _atomic_write_json(path: Path, data: dict | list):
    path.parent.mkdir(exist_ok=True)
    tmp_path = None
    try:
        with NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            json.dump(data, tmp, indent=2)
            tmp.write("\n")
        tmp_path.replace(path)
        tmp_path = None
    finally:
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def _write_last_error(message: str):
    _atomic_write_json(LAST_ERROR_FILE, {"at": _iso_now(), "error": message})


def _clear_last_error():
    LAST_ERROR_FILE.unlink(missing_ok=True)


def read_last_error() -> dict | None:
    try:
        if LAST_ERROR_FILE.exists():
            return json.loads(LAST_ERROR_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"error": "failed to read last_error.json"}
    return None


def read_run_stats() -> dict:
    try:
        if RUN_STATS_FILE.exists():
            return json.loads(RUN_STATS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"error": "failed to read run_stats.json"}
    return {}


def read_cache_metadata() -> dict:
    results_file = CACHE_DIR / "results.json"
    stats = read_run_stats()
    meta = {
        "cache_dir": str(CACHE_DIR),
        "has_results": results_file.exists(),
        "data_age_hours": None,
        "last_success_at": stats.get("last_success_at"),
        "last_attempt_at": stats.get("last_attempt_at"),
        "last_error": read_last_error(),
        "run_stats": stats,
        "degraded": False,
        "degraded_reasons": [],
    }
    if results_file.exists():
        age_hours = (time.time() - results_file.stat().st_mtime) / 3600
        meta["data_age_hours"] = round(age_hours, 2)
        meta["timestamp"] = _format_mtime(results_file)
        if age_hours > DATA_STALE_HOURS:
            meta["degraded"] = True
            meta["degraded_reasons"].append(f"results older than {DATA_STALE_HOURS:g} hours")
    else:
        meta["degraded"] = True
        meta["degraded_reasons"].append("no results cache")
    if meta["last_error"]:
        meta["degraded"] = True
        meta["degraded_reasons"].append("last refresh failed")
    return meta


# --- FMP budget tracking (persisted across restarts) ---

def _fmp_budget_file() -> Path:
    return CACHE_DIR / "fmp_budget.json"


def _fmp_budget() -> dict:
    f = _fmp_budget_file()
    if f.exists():
        data = json.loads(f.read_text(encoding="utf-8"))
        if data.get("date") == datetime.now().strftime("%Y-%m-%d"):
            return data
    return {"date": datetime.now().strftime("%Y-%m-%d"), "used": 0}


def _fmp_remaining() -> int:
    return max(0, FMP_DAILY_BUDGET - _fmp_budget()["used"])


def _fmp_increment(n: int = 1):
    b = _fmp_budget()
    b["used"] += n
    _atomic_write_json(_fmp_budget_file(), b)


_fmp_last_call = 0.0
_fmp_last_transient_error = False

def _fmp_had_transient_error() -> bool:
    return _fmp_last_transient_error

def _fmp_get(endpoint: str, params: dict) -> dict | list | str | None:
    global _fmp_last_call, _fmp_last_transient_error
    _fmp_last_transient_error = False
    if not FMP_API_KEY or _fmp_remaining() <= 0:
        return None
    # Rate limit: stay comfortably below FMP free-tier minute limits.
    elapsed = time.time() - _fmp_last_call
    if elapsed < 0.3:
        time.sleep(0.3 - elapsed)
    params = dict(params)
    params["apikey"] = FMP_API_KEY
    try:
        r = requests.get(f"{FMP_BASE}/{endpoint}", params=params, timeout=10)
        _fmp_last_call = time.time()
        _fmp_increment()
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            logger.warning("FMP rate limited")
            return "RATE_LIMITED"
        if r.status_code >= 500:
            _fmp_last_transient_error = True
        logger.debug("FMP %s returned status %s", endpoint, r.status_code)
    except Exception as e:
        _fmp_last_transient_error = True
        logger.debug("FMP %s error: %s", endpoint, e)
    return None


# --- Fundamentals cache ---

def _fundamentals_path(symbol: str) -> Path:
    return FUNDAMENTALS_DIR / f"{symbol}.json"


def _fmp_failure_path(symbol: str) -> Path:
    return CACHE_DIR / "fmp_failures" / f"{symbol}.fail"


def _is_fmp_failure_cached(symbol: str) -> bool:
    p = _fmp_failure_path(symbol)
    if not p.exists():
        return False
    age_days = (time.time() - p.stat().st_mtime) / 86400
    return age_days < FMP_FAILURE_CACHE_DAYS


def _save_fmp_failure(symbol: str):
    p = _fmp_failure_path(symbol)
    p.parent.mkdir(exist_ok=True)
    p.write_text(_iso_now(), encoding="utf-8")


def _load_fundamentals(symbol: str) -> dict | None:
    """Load cached fundamentals. Returns None if missing or stale."""
    p = _fundamentals_path(symbol)
    if not p.exists():
        return None
    age_days = (time.time() - p.stat().st_mtime) / 86400
    if age_days > FUNDAMENTALS_MAX_AGE_DAYS:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_fundamentals(symbol: str, data: dict):
    _atomic_write_json(_fundamentals_path(symbol), data)


# --- Yahoo screener (server-side pre-filter) ---

def get_candidates(force_refresh: bool = False) -> list[dict]:
    """Yahoo screener: returns list of {symbol, marketCap} for pre-filtered stocks."""
    cache_file = CACHE_DIR / "candidates.json"
    if cache_file.exists() and not force_refresh:
        age = time.time() - cache_file.stat().st_mtime
        if age < 86400:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            logger.info("Loaded %s cached candidates", len(data))
            return data

    q = EquityQuery("and", [
        EquityQuery("gt", ["intradaymarketcap", 3_000_000_000]),
        EquityQuery("gt", ["grossprofitmargin.lasttwelvemonths", 45]),
        EquityQuery("lt", ["netdebtebitda.lasttwelvemonths", 2.5]),
        EquityQuery("gt", ["leveredfreecashflow.lasttwelvemonths", 0]),
        EquityQuery("eq", ["region", "us"]),
    ])

    candidates = []
    offset = 0
    while True:
        result = screen(q, size=250, offset=offset)
        quotes = result.get("quotes", [])
        if not quotes:
            break
        for item in quotes:
            sym = item.get("symbol", "")
            exchange = item.get("exchange", "")
            if exchange in ("NMS", "NYQ", "NGM", "NCM", "ASE", "BTS", "PCX"):
                candidates.append({
                    "symbol": sym,
                    "marketCap": item.get("marketCap", 0),
                    "name": item.get("shortName") or item.get("longName") or sym,
                })
        total = result.get("total", 0)
        offset += 250
        if offset >= total:
            break
        time.sleep(0.2)

    _atomic_write_json(cache_file, candidates)
    logger.info("Fetched %s pre-filtered candidates", len(candidates))
    return candidates


def refresh_market_caps(candidates: list[dict]) -> dict[str, float]:
    """Get fresh market caps from screener data. Returns {symbol: marketCap}."""
    return {c["symbol"]: c["marketCap"] for c in candidates if c.get("marketCap", 0) > 0}


# --- Per-ticker fundamental extraction ---

def _safe_sum(series_list, key) -> float | None:
    total = 0.0
    for s in series_list:
        val = s.get(key)
        if val is None or pd.isna(val):
            return None
        total += val
    return total


def compute_cagr(latest: float, earliest: float, years: float) -> float | None:
    if earliest <= 0 or latest <= 0 or years <= 0:
        return None
    return (latest / earliest) ** (1 / years) - 1


def fetch_fundamentals_yahoo(symbol: str) -> dict | None:
    """Fetch and cache fundamental data from Yahoo. Returns static financial metrics."""
    try:
        t = yf.Ticker(symbol)

        # Quarterly for TTM
        q_inc = t.quarterly_income_stmt
        q_cf = t.quarterly_cashflow
        if q_inc is None or q_inc.empty or q_cf is None or q_cf.empty:
            return None
        if len(q_inc.columns) < 4 or len(q_cf.columns) < 4:
            return None

        q_inc_dates = sorted(q_inc.columns, reverse=True)[:4]
        q_cf_dates = sorted(q_cf.columns, reverse=True)[:4]

        ttm_revenue = _safe_sum([q_inc[d] for d in q_inc_dates], "Total Revenue")
        ttm_gross_profit = _safe_sum([q_inc[d] for d in q_inc_dates], "Gross Profit")
        ttm_ebitda = _safe_sum([q_inc[d] for d in q_inc_dates], "EBITDA")
        ttm_fcf = _safe_sum([q_cf[d] for d in q_cf_dates], "Free Cash Flow")

        if any(v is None for v in [ttm_revenue, ttm_gross_profit, ttm_fcf]):
            return None
        if ttm_revenue <= 0 or ttm_fcf <= 0:
            return None

        # Net Debt from balance sheet
        bs = t.balance_sheet
        if bs is None or bs.empty:
            return None
        net_debt = bs[sorted(bs.columns, reverse=True)[0]].get("Net Debt")
        if net_debt is None or pd.isna(net_debt):
            net_debt = 0.0

        # Annual for 3Y metrics
        ann_inc = t.income_stmt
        ann_cf = t.cashflow
        if ann_inc is None or ann_inc.empty or ann_cf is None or ann_cf.empty:
            return None

        ann_inc_dates = sorted(ann_inc.columns, reverse=True)
        ann_cf_dates = sorted(ann_cf.columns, reverse=True)
        if len(ann_inc_dates) < 4 or len(ann_cf_dates) < 3:
            return None

        # 3Y FCF Margin avg
        fcf_margins = []
        for i in range(min(3, len(ann_cf_dates), len(ann_inc_dates))):
            rev = ann_inc[ann_inc_dates[i]].get("Total Revenue")
            fcf = ann_cf[ann_cf_dates[i]].get("Free Cash Flow")
            if rev and not pd.isna(rev) and rev > 0 and fcf is not None and not pd.isna(fcf):
                fcf_margins.append(fcf / rev * 100)
        if len(fcf_margins) < 3:
            return None

        # Revenue CAGR 3Y
        rev_latest = ann_inc[ann_inc_dates[0]].get("Total Revenue")
        rev_3y = ann_inc[ann_inc_dates[3]].get("Total Revenue")
        if not rev_latest or pd.isna(rev_latest) or not rev_3y or pd.isna(rev_3y) or rev_3y <= 0:
            return None
        years_span = (ann_inc_dates[0] - ann_inc_dates[3]).days / 365.25
        revenue_cagr = compute_cagr(rev_latest, rev_3y, years_span)

        # EBIT CAGR 3Y
        ebit_latest = ann_inc[ann_inc_dates[0]].get("EBIT")
        ebit_3y = ann_inc[ann_inc_dates[3]].get("EBIT")
        if not ebit_latest or pd.isna(ebit_latest) or not ebit_3y or pd.isna(ebit_3y) or ebit_3y <= 0:
            return None
        ebit_cagr = compute_cagr(ebit_latest, ebit_3y, years_span)

        if revenue_cagr is None or ebit_cagr is None:
            return None

        info = t.info or {}

        return {
            "symbol": symbol,
            "name": info.get("shortName") or info.get("longName") or symbol,
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "source": "yahoo",
            "fetched_at": _iso_now(),
            # Static fundamentals (change quarterly)
            "ttm_revenue": ttm_revenue,
            "ttm_gross_profit": ttm_gross_profit,
            "ttm_ebitda": ttm_ebitda,
            "ttm_fcf": ttm_fcf,
            "net_debt": net_debt,
            "gross_margin_pct": round(ttm_gross_profit / ttm_revenue * 100, 2),
            "fcf_margin_3y_avg_pct": round(sum(fcf_margins) / len(fcf_margins), 2),
            "revenue_cagr_pct": round(revenue_cagr * 100, 2),
            "ebit_cagr_pct": round(ebit_cagr * 100, 2),
            "net_debt_ebitda": round(net_debt / ttm_ebitda, 1) if ttm_ebitda and ttm_ebitda > 0 else 0.0,
        }
    except Exception as e:
        logger.debug("%s: yahoo error - %s", symbol, e)
        return None


def fetch_fundamentals_fmp(symbol: str) -> dict | str | None:
    """Fetch fundamentals from FMP. Costs ~6 API calls.

    Returns dict on success, "RATE_LIMITED" if throttled, "TRANSIENT_ERROR" for temporary
    network/server errors, None on persistent data errors.
    """
    if _fmp_remaining() < 6:
        return None

    profile = _fmp_get("profile", {"symbol": symbol})
    if profile == "RATE_LIMITED":
        return "RATE_LIMITED"
    if _fmp_had_transient_error():
        return "TRANSIENT_ERROR"
    if not profile or not isinstance(profile, list) or not profile:
        return None
    profile = profile[0]

    # Try financial statements
    results = []
    for ep, params in [
        ("income-statement", {"symbol": symbol, "period": "annual", "limit": "4"}),
        ("cash-flow-statement", {"symbol": symbol, "period": "annual", "limit": "3"}),
        ("income-statement", {"symbol": symbol, "period": "quarter", "limit": "4"}),
        ("cash-flow-statement", {"symbol": symbol, "period": "quarter", "limit": "4"}),
    ]:
        data = _fmp_get(ep, params)
        if data == "RATE_LIMITED":
            return "RATE_LIMITED"
        if _fmp_had_transient_error():
            return "TRANSIENT_ERROR"
        results.append(data)

    inc, cf, q_inc, q_cf = results
    if not all(isinstance(x, list) and x for x in [inc, cf, q_inc, q_cf]):
        return None

    if len(inc) < 4 or len(cf) < 3 or len(q_inc) < 4 or len(q_cf) < 4:
        return None

    # TTM from quarterly
    ttm_revenue = sum(q.get("revenue", 0) for q in q_inc[:4])
    ttm_gross_profit = sum(q.get("grossProfit", 0) for q in q_inc[:4])
    ttm_ebitda = sum(q.get("ebitda", 0) for q in q_inc[:4])
    ttm_fcf = sum(q.get("freeCashFlow", 0) for q in q_cf[:4])

    if ttm_revenue <= 0 or ttm_fcf <= 0:
        return None

    # Net Debt from most recent balance sheet
    bs = _fmp_get("balance-sheet-statement", {"symbol": symbol, "limit": "1"})
    if _fmp_had_transient_error():
        return "TRANSIENT_ERROR"
    net_debt = 0.0
    if bs and isinstance(bs, list) and bs:
        net_debt = bs[0].get("netDebt", 0) or 0

    # 3Y FCF margin avg
    fcf_margins = []
    for i in range(min(3, len(cf), len(inc))):
        rev = inc[i].get("revenue", 0)
        fcf = cf[i].get("freeCashFlow", 0)
        if rev > 0:
            fcf_margins.append(fcf / rev * 100)
    if len(fcf_margins) < 3:
        return None

    # CAGR
    rev_latest = inc[0].get("revenue", 0)
    rev_3y = inc[3].get("revenue", 0)
    ebit_latest = inc[0].get("ebit", 0)
    ebit_3y = inc[3].get("ebit", 0)

    if rev_3y <= 0 or ebit_3y <= 0 or rev_latest <= 0 or ebit_latest <= 0:
        return None

    revenue_cagr = compute_cagr(rev_latest, rev_3y, 3.0)
    ebit_cagr = compute_cagr(ebit_latest, ebit_3y, 3.0)

    if revenue_cagr is None or ebit_cagr is None:
        return None

    return {
        "symbol": symbol,
        "name": profile.get("companyName", symbol),
        "sector": profile.get("sector", ""),
        "industry": profile.get("industry", ""),
        "source": "fmp",
        "fetched_at": _iso_now(),
        "ttm_revenue": ttm_revenue,
        "ttm_gross_profit": ttm_gross_profit,
        "ttm_ebitda": ttm_ebitda,
        "ttm_fcf": ttm_fcf,
        "net_debt": net_debt,
        "gross_margin_pct": round(ttm_gross_profit / ttm_revenue * 100, 2),
        "fcf_margin_3y_avg_pct": round(sum(fcf_margins) / len(fcf_margins), 2),
        "revenue_cagr_pct": round((revenue_cagr or 0) * 100, 2),
        "ebit_cagr_pct": round((ebit_cagr or 0) * 100, 2),
        "net_debt_ebitda": round(net_debt / ttm_ebitda, 1) if ttm_ebitda and ttm_ebitda > 0 else 0.0,
    }


# --- Screening logic ---

def score_ticker(fundamentals: dict, market_cap: float) -> dict | None:
    """Combine cached fundamentals with fresh market cap. Returns screened result or None."""
    ttm_fcf = fundamentals.get("ttm_fcf", 0)
    if ttm_fcf <= 0 or market_cap <= 0:
        return None

    p_fcf = market_cap / ttm_fcf

    result = {
        "symbol": fundamentals["symbol"],
        "name": fundamentals.get("name", ""),
        "sector": fundamentals.get("sector", ""),
        "industry": fundamentals.get("industry", ""),
        "source": fundamentals.get("source", ""),
        "market_cap_b": round(market_cap / 1e9, 2),
        "gross_margin_pct": fundamentals["gross_margin_pct"],
        "fcf_margin_3y_avg_pct": fundamentals["fcf_margin_3y_avg_pct"],
        "p_fcf": round(p_fcf, 1),
        "revenue_cagr_pct": fundamentals["revenue_cagr_pct"],
        "ebit_cagr_pct": fundamentals["ebit_cagr_pct"],
        "net_debt_ebitda": fundamentals["net_debt_ebitda"],
    }

    # Apply all filters
    if not (
        result["gross_margin_pct"] > 45.0
        and result["fcf_margin_3y_avg_pct"] > 15.0
        and result["revenue_cagr_pct"] > 3.0
        and 7.0 <= result["p_fcf"] <= 17.0
        and result["ebit_cagr_pct"] > 0.0
        and result["net_debt_ebitda"] < 2.5
    ):
        return None

    return result


def _rank_fmp_candidates(yahoo_failures: list[str], candidates: list[dict]) -> list[str]:
    """Spend scarce FMP calls on larger unresolved candidates first."""
    by_symbol = {c["symbol"]: c for c in candidates}
    ranked = []
    for sym in yahoo_failures:
        c = by_symbol.get(sym, {})
        market_cap = c.get("marketCap") or 0
        if market_cap >= FMP_MIN_MARKET_CAP and not _is_fmp_failure_cached(sym):
            ranked.append((market_cap, sym))
    ranked.sort(reverse=True)
    return [sym for _, sym in ranked]


def run_screen(max_workers: int = 6, progress_callback=None, force_candidates: bool = False) -> dict:
    """Full screening run with caching."""
    attempt_at = _iso_now()
    stats = {
        "last_attempt_at": attempt_at,
        "last_success_at": None,
        "candidates": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "yahoo_failures": 0,
        "fmp_candidates": 0,
        "fmp_attempted": 0,
        "fmp_successes": 0,
        "fmp_transient_errors": 0,
        "fmp_budget_used": _fmp_budget().get("used", 0),
        "fmp_budget_remaining": _fmp_remaining(),
        "results": 0,
        "excluded": 0,
        "pending": 0,
    }
    _atomic_write_json(RUN_STATS_FILE, stats)

    try:
        candidates = get_candidates(force_refresh=force_candidates)
        market_caps = refresh_market_caps(candidates)
        symbols = [c["symbol"] for c in candidates]
        stats["candidates"] = len(symbols)
        logger.info("Screening %s pre-filtered candidates...", len(symbols))

        # Phase 1: Load cached fundamentals, identify missing
        cached = {}
        missing = []
        for sym in symbols:
            f = _load_fundamentals(sym)
            if f:
                cached[sym] = f
            else:
                missing.append(sym)

        stats["cache_hits"] = len(cached)
        stats["cache_misses"] = len(missing)
        logger.info("Cache: %s hit, %s miss", len(cached), len(missing))

        # Phase 2: Fetch missing from Yahoo in batches to avoid rate limits
        completed = len(cached)
        total = len(symbols)

        if progress_callback:
            progress_callback(completed, total)

        def _fetch_batch(syms, workers, pause_between=0):
            """Fetch a batch of tickers. Returns list of symbols that failed."""
            failures = []
            nonlocal completed
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(fetch_fundamentals_yahoo, sym): sym for sym in syms}
                for future in as_completed(futures):
                    sym = futures[future]
                    completed += 1
                    if progress_callback:
                        progress_callback(completed, total)
                    data = future.result()
                    if data:
                        _save_fundamentals(sym, data)
                        cached[sym] = data
                    else:
                        failures.append(sym)
            if pause_between > 0 and failures:
                time.sleep(pause_between)
            return failures

        # First pass: 6 workers, batches of 50 with 2s pauses
        yahoo_failures = []
        batch_size = 50
        for i in range(0, len(missing), batch_size):
            batch = missing[i:i + batch_size]
            fails = _fetch_batch(batch, workers=max_workers)
            yahoo_failures.extend(fails)
            if i + batch_size < len(missing):
                time.sleep(2)

        # Retry pass: failed tickers with 2 workers and 3s pause (gentler on Yahoo)
        if yahoo_failures:
            logger.info("Retrying %s Yahoo failures with lower concurrency...", len(yahoo_failures))
            time.sleep(5)
            still_failing = _fetch_batch(yahoo_failures, workers=2, pause_between=3)
            yahoo_failures = still_failing

        stats["yahoo_failures"] = len(yahoo_failures)

        # Phase 3: FMP backfill for persistent Yahoo failures. Keep this cheap.
        try:
            if FMP_API_KEY and yahoo_failures:
                fmp_candidates = _rank_fmp_candidates(yahoo_failures, candidates)
                budget = _fmp_remaining()
                can_process = min(len(fmp_candidates), budget // 6, FMP_MAX_TICKERS_PER_RUN)
                stats["fmp_candidates"] = len(fmp_candidates)
                if can_process > 0:
                    logger.info(
                        "FMP backfill: %s/%s prioritized failures (%s calls remaining)",
                        can_process,
                        len(fmp_candidates),
                        budget,
                    )
                    fmp_successes = 0
                    fmp_attempted = 0
                    for sym in fmp_candidates[:can_process]:
                        if _fmp_remaining() < 6:
                            logger.warning("Stopping FMP backfill: remaining budget is too low")
                            break
                        data = fetch_fundamentals_fmp(sym)
                        fmp_attempted += 1
                        if data == "RATE_LIMITED":
                            logger.warning("FMP rate limited after %s tickers, stopping backfill", fmp_successes)
                            break
                        if data == "TRANSIENT_ERROR":
                            stats["fmp_transient_errors"] = stats.get("fmp_transient_errors", 0) + 1
                            logger.warning(
                                "Stopping FMP backfill after transient error for %s; not caching failure",
                                sym,
                            )
                            break
                        if isinstance(data, dict):
                            _save_fundamentals(sym, data)
                            cached[sym] = data
                            fmp_successes += 1
                        else:
                            _save_fmp_failure(sym)

                        if fmp_attempted >= FMP_MIN_ATTEMPTS_BEFORE_STOP:
                            success_rate = fmp_successes / fmp_attempted
                            if success_rate < FMP_MIN_SUCCESS_RATE:
                                logger.warning(
                                    "Stopping FMP backfill early: %s/%s successes below %.0f%% threshold",
                                    fmp_successes,
                                    fmp_attempted,
                                    FMP_MIN_SUCCESS_RATE * 100,
                                )
                                break
                    stats["fmp_attempted"] = fmp_attempted
                    stats["fmp_successes"] = fmp_successes
                    logger.info("FMP backfill complete: %s/%s tickers cached", fmp_successes, fmp_attempted)
        except Exception as e:
            logger.error("FMP backfill failed (non-fatal): %s", e)

        # Phase 4: Score with fresh market caps
        results = []
        excluded = []
        pending = []
        candidate_names = {c["symbol"]: c.get("name", c["symbol"]) for c in candidates}

        for sym in symbols:
            mc = market_caps.get(sym, 0)
            if sym in cached:
                result = score_ticker(cached[sym], mc)
                if result:
                    results.append(result)
                else:
                    excluded.append({
                        "symbol": sym,
                        "name": cached[sym].get("name", candidate_names.get(sym, sym)),
                        "source": "excluded",
                    })
            else:
                pending.append({
                    "symbol": sym,
                    "name": candidate_names.get(sym, sym),
                    "source": "no_data",
                })

        results.sort(key=lambda x: x["p_fcf"])

        stats.update({
            "last_success_at": _iso_now(),
            "fmp_budget_used": _fmp_budget().get("used", 0),
            "fmp_budget_remaining": _fmp_remaining(),
            "results": len(results),
            "excluded": len(excluded),
            "pending": len(pending),
            "fundamentals_available": len(cached),
        })
        _atomic_write_json(RUN_STATS_FILE, stats)
        _clear_last_error()
        logger.info(
            "Screen complete: %s passed, %s excluded, %s no data, %s/%s have fundamentals, "
            "%s Yahoo failures, FMP: %s attempted/%s successes, budget: %s/%s remaining",
            len(results), len(excluded), len(pending), len(cached), len(symbols), len(yahoo_failures),
            stats["fmp_attempted"], stats["fmp_successes"], stats["fmp_budget_remaining"], FMP_DAILY_BUDGET,
        )
        return {"results": results, "excluded": excluded, "pending": pending}
    except Exception as e:
        _write_last_error(str(e))
        stats["last_error"] = str(e)
        _atomic_write_json(RUN_STATS_FILE, stats)
        raise


def run_screen_cached(max_age_hours: int = 1, progress_callback=None, force: bool = False) -> tuple[dict, str]:
    """Returns ({"results": [...], "pending": [...]}, timestamp)."""
    cache_file = CACHE_DIR / "results.json"

    if cache_file.exists() and not force:
        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < max_age_hours:
            cached = json.loads(cache_file.read_text(encoding="utf-8"))
            ts = _format_mtime(cache_file)
            logger.info("Using cached results from %s", ts)
            return cached, ts

    data = run_screen(progress_callback=progress_callback, force_candidates=force)
    _atomic_write_json(cache_file, data)
    ts = _format_mtime(cache_file)
    return data, ts


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the value stock screener refresh")
    parser.add_argument("--force", action="store_true", help="ignore results/candidate cache and refresh now")
    parser.add_argument("--max-age-hours", type=int, default=1)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    data, ts = run_screen_cached(max_age_hours=args.max_age_hours, force=args.force)
    print(json.dumps({
        "ok": True,
        "timestamp": ts,
        "results": len(data.get("results", [])),
        "excluded": len(data.get("excluded", [])),
        "pending": len(data.get("pending", [])),
        "metadata": read_cache_metadata(),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
