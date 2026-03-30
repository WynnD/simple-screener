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
4. Lazy-load from FMP for persistent Yahoo failures (respects 250/day budget)
5. Combine fundamentals + fresh market cap → apply all filters
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from yfinance.screener import EquityQuery, screen

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)
FUNDAMENTALS_DIR = CACHE_DIR / "fundamentals"
FUNDAMENTALS_DIR.mkdir(exist_ok=True)

FUNDAMENTALS_MAX_AGE_DAYS = 30
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"


# --- FMP budget tracking (persisted across restarts) ---

def _fmp_budget_file() -> Path:
    return CACHE_DIR / "fmp_budget.json"


def _fmp_budget() -> dict:
    f = _fmp_budget_file()
    if f.exists():
        data = json.loads(f.read_text())
        if data.get("date") == datetime.now().strftime("%Y-%m-%d"):
            return data
    return {"date": datetime.now().strftime("%Y-%m-%d"), "used": 0}


def _fmp_remaining() -> int:
    return max(0, 250 - _fmp_budget()["used"])


def _fmp_increment(n: int = 1):
    b = _fmp_budget()
    b["used"] += n
    _fmp_budget_file().write_text(json.dumps(b))


def _fmp_get(endpoint: str, params: dict) -> dict | list | None:
    if not FMP_API_KEY or _fmp_remaining() <= 0:
        return None
    params["apikey"] = FMP_API_KEY
    try:
        r = requests.get(f"{FMP_BASE}/{endpoint}", params=params, timeout=10)
        _fmp_increment()
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


# --- Fundamentals cache ---

def _fundamentals_path(symbol: str) -> Path:
    return FUNDAMENTALS_DIR / f"{symbol}.json"


def _load_fundamentals(symbol: str) -> dict | None:
    """Load cached fundamentals. Returns None if missing or stale."""
    p = _fundamentals_path(symbol)
    if not p.exists():
        return None
    age_days = (time.time() - p.stat().st_mtime) / 86400
    if age_days > FUNDAMENTALS_MAX_AGE_DAYS:
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _save_fundamentals(symbol: str, data: dict):
    _fundamentals_path(symbol).write_text(json.dumps(data))


# --- Yahoo screener (server-side pre-filter) ---

def get_candidates() -> list[dict]:
    """Yahoo screener: returns list of {symbol, marketCap} for pre-filtered stocks."""
    cache_file = CACHE_DIR / "candidates.json"
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < 86400:
            data = json.loads(cache_file.read_text())
            logger.info(f"Loaded {len(data)} cached candidates")
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

    cache_file.write_text(json.dumps(candidates))
    logger.info(f"Fetched {len(candidates)} pre-filtered candidates")
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
            "fetched_at": datetime.now().isoformat(),
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
        logger.debug(f"{symbol}: yahoo error - {e}")
        return None


def fetch_fundamentals_fmp(symbol: str) -> dict | None:
    """Fetch fundamentals from FMP. Costs ~5 API calls."""
    if _fmp_remaining() < 5:
        return None

    profile = _fmp_get("profile", {"symbol": symbol})
    if not profile or not isinstance(profile, list) or not profile:
        return None
    profile = profile[0]

    # Try financial statements
    inc = _fmp_get("income-statement", {"symbol": symbol, "period": "annual", "limit": "4"})
    cf = _fmp_get("cash-flow-statement", {"symbol": symbol, "period": "annual", "limit": "3"})
    q_inc = _fmp_get("income-statement", {"symbol": symbol, "period": "quarter", "limit": "4"})
    q_cf = _fmp_get("cash-flow-statement", {"symbol": symbol, "period": "quarter", "limit": "4"})

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
        "fetched_at": datetime.now().isoformat(),
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


def run_screen(max_workers: int = 6, progress_callback=None) -> list[dict]:
    """Full screening run with caching."""
    candidates = get_candidates()
    market_caps = refresh_market_caps(candidates)
    symbols = [c["symbol"] for c in candidates]
    logger.info(f"Screening {len(symbols)} pre-filtered candidates...")

    # Phase 1: Load cached fundamentals, identify missing
    cached = {}
    missing = []
    for sym in symbols:
        f = _load_fundamentals(sym)
        if f:
            cached[sym] = f
        else:
            missing.append(sym)

    logger.info(f"Cache: {len(cached)} hit, {len(missing)} miss")

    # Phase 2: Fetch missing from Yahoo
    completed = len(cached)
    total = len(symbols)

    if progress_callback:
        progress_callback(completed, total)

    yahoo_failures = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_fundamentals_yahoo, sym): sym for sym in missing}
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
                yahoo_failures.append(sym)

    # Phase 3: Lazy FMP backfill for Yahoo failures
    if FMP_API_KEY and yahoo_failures:
        budget = _fmp_remaining()
        can_process = min(len(yahoo_failures), budget // 6)  # ~6 calls per ticker
        if can_process > 0:
            logger.info(f"FMP backfill: {can_process}/{len(yahoo_failures)} failures ({budget} calls remaining)")
            for sym in yahoo_failures[:can_process]:
                data = fetch_fundamentals_fmp(sym)
                if data:
                    _save_fundamentals(sym, data)
                    cached[sym] = data

    # Phase 4: Score with fresh market caps
    results = []
    pending = []
    candidate_names = {c["symbol"]: c.get("name", c["symbol"]) for c in candidates}

    for sym in symbols:
        mc = market_caps.get(sym, 0)
        if sym in cached:
            result = score_ticker(cached[sym], mc)
            if result:
                results.append(result)
        else:
            # No fundamentals yet — show as pending
            pending.append({
                "symbol": sym,
                "name": candidate_names.get(sym, sym),
                "sector": "",
                "industry": "",
                "source": "pending",
                "market_cap_b": round(mc / 1e9, 2) if mc else None,
                "gross_margin_pct": None,
                "fcf_margin_3y_avg_pct": None,
                "p_fcf": None,
                "revenue_cagr_pct": None,
                "ebit_cagr_pct": None,
                "net_debt_ebitda": None,
            })

    results.sort(key=lambda x: x["p_fcf"])

    yahoo_fail_count = len(yahoo_failures)
    fmp_budget_left = _fmp_remaining()
    logger.info(
        f"Screen complete: {len(results)} passed, {len(pending)} pending, "
        f"{len(cached)}/{len(symbols)} have fundamentals, "
        f"{yahoo_fail_count} Yahoo failures, "
        f"FMP budget: {fmp_budget_left}/250 remaining"
    )
    return {"results": results, "pending": pending}


def run_screen_cached(max_age_hours: int = 1, progress_callback=None) -> tuple[dict, str]:
    """Returns ({"results": [...], "pending": [...]}, timestamp)."""
    cache_file = CACHE_DIR / "results.json"

    if cache_file.exists():
        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < max_age_hours:
            cached = json.loads(cache_file.read_text())
            ts = datetime.fromtimestamp(cache_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            logger.info(f"Using cached results from {ts}")
            return cached, ts

    data = run_screen(progress_callback=progress_callback)
    cache_file.write_text(json.dumps(data, indent=2))
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    return data, ts
