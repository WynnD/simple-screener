"""Flask web app for the stock screener."""

import logging
import threading

from flask import Flask, jsonify, render_template_string

from screener import CACHE_DIR, read_cache_metadata, run_screen_cached

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

_cache_lock = threading.Lock()


# In-memory state for background screening
_state = {
    "results": None,
    "excluded": None,
    "pending": None,
    "timestamp": None,
    "running": False,
    "progress": 0,
    "total": 0,
    "metadata": None,
    "last_loaded_mtime": 0.0,
}


def _load_stale_cache():
    """Load disk cache when the CronJob or manual refresh has updated it."""
    import json
    from datetime import datetime, timezone

    cache_file = CACHE_DIR / "results.json"
    stats_file = CACHE_DIR / "run_stats.json"
    mtime = 0.0
    if cache_file.exists():
        mtime = max(mtime, cache_file.stat().st_mtime)
    if stats_file.exists():
        mtime = max(mtime, stats_file.stat().st_mtime)

    with _cache_lock:
        if mtime <= 0.0 or _state["last_loaded_mtime"] >= mtime:
            return _state["metadata"]

        try:
            if cache_file.exists():
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                ts = datetime.fromtimestamp(cache_file.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                _state["results"] = data.get("results", data if isinstance(data, list) else [])
                _state["excluded"] = data.get("excluded", [])
                _state["pending"] = data.get("pending", [])
                _state["timestamp"] = ts
            _state["metadata"] = read_cache_metadata()
            _state["last_loaded_mtime"] = mtime
            logger.info(f"Loaded cache: {len(_state['results'] or [])} results")
        except Exception as e:
            logger.error(f"Failed to load stale cache: {e}")
        return _state["metadata"]


def _progress_cb(done, total):
    _state["progress"] = done
    _state["total"] = total


def _run_background():
    _state["running"] = True
    _state["progress"] = 0
    try:
        data, ts = run_screen_cached(max_age_hours=1, progress_callback=_progress_cb, force=True)
        _state["results"] = data.get("results", [])
        _state["excluded"] = data.get("excluded", [])
        _state["pending"] = data.get("pending", [])
        _state["timestamp"] = ts
        _state["metadata"] = read_cache_metadata()
    except Exception as e:
        logger.error(f"Screen failed: {e}")
        _state["metadata"] = read_cache_metadata()
    finally:
        _state["running"] = False


# Load cached results immediately so the UI has data right away. Refresh ownership
# belongs to the Kubernetes CronJob; the web process only refreshes on demand.
_load_stale_cache()

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stock Screener</title>
<style>
  :root {
    --bg: #0a0a0a; --surface: #141414; --border: #262626;
    --text: #e5e5e5; --muted: #737373; --accent: #3b82f6;
    --green: #22c55e; --red: #ef4444;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Geist Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.5;
    padding: 2rem; max-width: 1400px; margin: 0 auto;
  }
  h1 { font-size: 1.5rem; font-weight: 600; margin-bottom: 0.25rem; }
  .subtitle { color: var(--muted); font-size: 0.85rem; margin-bottom: 0.75rem; }
  .health {
    display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 1.5rem;
    font-size: 0.8rem;
  }
  .pill { border: 1px solid var(--border); border-radius: 999px; padding: 0.2rem 0.65rem; color: var(--muted); }
  .pill.ok { color: var(--green); border-color: rgba(34, 197, 94, 0.35); }
  .pill.warn { color: #f59e0b; border-color: rgba(245, 158, 11, 0.35); }
  .pill.bad { color: var(--red); border-color: rgba(239, 68, 68, 0.35); }
  .criteria {
    display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 1.5rem;
  }
  .chip {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.25rem 0.75rem; font-size: 0.8rem;
    font-family: 'Geist Mono', 'SF Mono', monospace; color: var(--muted);
  }
  .status { color: var(--muted); font-size: 0.85rem; margin-bottom: 1rem; }
  .progress-bar {
    width: 300px; height: 4px; background: var(--border); border-radius: 2px;
    margin: 0.5rem 0; overflow: hidden;
  }
  .progress-fill {
    height: 100%; background: var(--accent); transition: width 0.3s;
  }
  table {
    width: 100%; border-collapse: collapse; font-size: 0.85rem;
  }
  thead { position: sticky; top: 0; }
  th {
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 0.6rem 0.75rem; text-align: left; font-weight: 500;
    color: var(--muted); font-size: 0.75rem; text-transform: uppercase;
    letter-spacing: 0.05em; cursor: pointer; user-select: none;
    white-space: nowrap;
  }
  th:hover { color: var(--text); }
  th.sorted-asc::after { content: ' \\25B2'; font-size: 0.65rem; }
  th.sorted-desc::after { content: ' \\25BC'; font-size: 0.65rem; }
  td {
    padding: 0.5rem 0.75rem; border-bottom: 1px solid var(--border);
    font-family: 'Geist Mono', 'SF Mono', monospace; font-size: 0.8rem;
  }
  tr:hover td { background: var(--surface); }
  .sym { font-weight: 600; color: var(--accent); }
  .name { color: var(--muted); font-family: 'Geist Sans', sans-serif; max-width: 200px;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .num { text-align: right; }
  .good { color: var(--green); }
  .btn {
    background: var(--surface); border: 1px solid var(--border); color: var(--text);
    padding: 0.4rem 1rem; border-radius: 6px; cursor: pointer; font-size: 0.85rem;
  }
  .btn:hover { border-color: var(--accent); }
  .btn:disabled { opacity: 0.4; cursor: not-allowed; }
  .header-row { display: flex; align-items: center; gap: 1rem; margin-bottom: 0.25rem; }
  .count { color: var(--accent); font-weight: 600; }
  .table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
  @media (max-width: 900px) {
    body { padding: 0.75rem; }
    table { font-size: 0.78rem; }
    td, th { padding: 0.4rem 0.5rem; white-space: nowrap; }
  }
</style>
</head>
<body>
  <div class="header-row">
    <h1>Stock Screener</h1>
    <button class="btn" id="refresh" onclick="refresh()">Refresh</button>
    <button class="btn" onclick="exportCsv()">Export CSV</button>
  </div>
  <p class="subtitle" id="meta">Loading...</p>
  <div class="health" id="health"></div>
  <div class="criteria">
    <span class="chip">Mkt Cap &gt; $3B</span>
    <span class="chip">FCF Margin 3Y Avg &gt; 15%</span>
    <span class="chip">Gross Margin &gt; 45%</span>
    <span class="chip">Rev CAGR 3Y &gt; 3%</span>
    <span class="chip">P/FCF 7x&ndash;17x</span>
    <span class="chip">EBIT CAGR 3Y &gt; 0%</span>
    <span class="chip">Net Debt/EBITDA &lt; 2.5x</span>
  </div>
  <div id="progress-section" style="display:none">
    <p class="status" id="progress-text">Screening...</p>
    <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
  </div>
  <div class="table-wrap">
  <table>
    <thead>
      <tr>
        <th data-key="symbol">Ticker</th>
        <th data-key="name">Name</th>
        <th data-key="sector">Sector</th>
        <th data-key="market_cap_b" class="num">Mkt Cap ($B)</th>
        <th data-key="gross_margin_pct" class="num">Gross Margin</th>
        <th data-key="fcf_margin_3y_avg_pct" class="num">FCF Margin 3Y</th>
        <th data-key="p_fcf" class="num">P/FCF</th>
        <th data-key="revenue_cagr_pct" class="num">Rev CAGR 3Y</th>
        <th data-key="ebit_cagr_pct" class="num">EBIT CAGR 3Y</th>
        <th data-key="net_debt_ebitda" class="num">ND/EBITDA</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
  </div>

<script>
let sortKey = 'p_fcf', sortAsc = true, data = [], excluded = [], pending = [];

function fmt(val, suffix) {
  if (val === null || val === undefined) return '\u2026';
  return val.toFixed(suffix === 'x2' ? 2 : 1) + (suffix === 'x2' ? 'x' : suffix || '');
}

function renderRow(r, isPending) {
  const tr = document.createElement('tr');
  if (isPending) tr.style.opacity = '0.45';
  const cells = [
    { cls: 'sym', text: r.symbol },
    { cls: 'name', text: r.name || '' },
    { cls: 'name', text: r.sector || '' },
    { cls: 'num', text: fmt(r.market_cap_b, '') },
    { cls: 'num', text: fmt(r.gross_margin_pct, '%') },
    { cls: 'num', text: fmt(r.fcf_margin_3y_avg_pct, '%') },
    { cls: isPending ? 'num' : 'num good', text: fmt(r.p_fcf, 'x') },
    { cls: 'num', text: fmt(r.revenue_cagr_pct, '%') },
    { cls: 'num', text: fmt(r.ebit_cagr_pct, '%') },
    { cls: 'num', text: fmt(r.net_debt_ebitda, 'x2') },
  ];
  cells.forEach(c => {
    const td = document.createElement('td');
    td.className = c.cls;
    td.textContent = c.text;
    tr.appendChild(td);
  });
  return tr;
}

function renderHealth(metadata) {
  const el = document.getElementById('health');
  el.textContent = '';
  if (!metadata) return;
  const stats = metadata.run_stats || {};
  const pills = [
    { text: metadata.degraded ? 'DEGRADED' : 'fresh enough', cls: metadata.degraded ? 'bad' : 'ok' },
    { text: metadata.data_age_hours == null ? 'no age' : 'age ' + metadata.data_age_hours + 'h', cls: metadata.degraded ? 'warn' : 'ok' },
    { text: 'FMP left ' + (stats.fmp_budget_remaining ?? 'unknown'), cls: (stats.fmp_budget_remaining ?? 999) < 50 ? 'warn' : 'ok' },
    { text: 'FMP tried ' + (stats.fmp_attempted ?? 0) + ' / hit ' + (stats.fmp_successes ?? 0), cls: '' },
  ];
  if (metadata.last_error) pills.push({ text: 'last error: ' + metadata.last_error.error, cls: 'bad' });
  (metadata.degraded_reasons || []).forEach(r => pills.push({ text: r, cls: 'warn' }));
  pills.forEach(p => {
    const span = document.createElement('span');
    span.className = 'pill ' + p.cls;
    span.textContent = p.text;
    el.appendChild(span);
  });
}

function render() {
  const sorted = [...data].sort((a, b) => {
    let va = a[sortKey], vb = b[sortKey];
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (typeof va === 'string') return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
    return sortAsc ? va - vb : vb - va;
  });
  const tbody = document.getElementById('tbody');
  tbody.textContent = '';
  sorted.forEach(r => tbody.appendChild(renderRow(r, false)));

  document.querySelectorAll('th').forEach(th => {
    th.classList.remove('sorted-asc', 'sorted-desc');
    if (th.dataset.key === sortKey) th.classList.add(sortAsc ? 'sorted-asc' : 'sorted-desc');
  });
}

document.querySelectorAll('th').forEach(th => {
  th.addEventListener('click', () => {
    if (th.dataset.key === sortKey) sortAsc = !sortAsc;
    else { sortKey = th.dataset.key; sortAsc = true; }
    render();
  });
});

async function poll() {
  const res = await fetch('/api/status');
  const s = await res.json();

  if (s.running) {
    document.getElementById('progress-section').style.display = '';
    const pct = s.total > 0 ? (s.progress / s.total * 100) : 0;
    document.getElementById('progress-fill').style.width = pct + '%';
    document.getElementById('progress-text').textContent =
      'Screening ' + s.progress + ' / ' + s.total + ' tickers...';
    document.getElementById('refresh').disabled = true;
    setTimeout(poll, 2000);
  } else {
    document.getElementById('progress-section').style.display = 'none';
    document.getElementById('refresh').disabled = false;
    if (s.results) {
      data = s.results;
      excluded = s.excluded || [];
      pending = s.pending || [];
      let meta = data.length + ' stocks passing';
      if (excluded.length > 0) meta += ' \u00b7 ' + excluded.length + ' excluded';
      if (pending.length > 0) meta += ' \u00b7 ' + pending.length + ' no data';
      meta += ' · Updated ' + s.timestamp;
      document.getElementById('meta').textContent = meta;
      renderHealth(s.metadata);
      render();
    } else {
      document.getElementById('meta').textContent = 'No results yet. Click Refresh.';
      renderHealth(s.metadata);
    }
  }
}

async function refresh() {
  await fetch('/api/refresh', { method: 'POST' });
  poll();
}

function exportCsv() {
  if (!data.length) return;
  const cols = ['symbol','name','sector','market_cap_b','gross_margin_pct','fcf_margin_3y_avg_pct','p_fcf','revenue_cagr_pct','ebit_cagr_pct','net_debt_ebitda'];
  const header = ['Ticker','Name','Sector','Mkt Cap ($B)','Gross Margin %','FCF Margin 3Y %','P/FCF','Rev CAGR 3Y %','EBIT CAGR 3Y %','ND/EBITDA'];
  const rows = data.map(r => cols.map(c => {
    const v = r[c];
    if (v == null) return '';
    if (typeof v === 'string') return '"' + v.replace(/"/g, '""') + '"';
    return v;
  }).join(','));
  const csv = [header.join(','), ...rows].join(String.fromCharCode(10));
  const blob = new Blob([csv], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'screener_' + new Date().toISOString().slice(0,10) + '.csv';
  a.click();
}

poll();
</script>
</body>
</html>"""


@app.route("/")
def index():
    resp = app.make_response(render_template_string(HTML_TEMPLATE))
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp


@app.route("/api/status")
def api_status():
    _load_stale_cache()
    return jsonify({
        "running": _state["running"],
        "progress": _state["progress"],
        "total": _state["total"],
        "results": _state["results"],
        "excluded": _state["excluded"],
        "pending": _state["pending"],
        "timestamp": _state["timestamp"],
        "metadata": _state["metadata"],
    })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if _state["running"]:
        return jsonify({"ok": False, "msg": "Already running"})

    # Delete caches to force full re-fetch
    from pathlib import Path
    cache_dir = Path(__file__).parent / "cache"
    for name in ("results.json", "candidates.json"):
        f = cache_dir / name
        if f.exists():
            f.unlink()

    threading.Thread(target=_run_background, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/health")
def health():
    return "ok"


@app.route("/readyz")
def readyz():
    _load_stale_cache()
    metadata = _state["metadata"] or read_cache_metadata()
    status = 200 if not metadata.get("degraded") else 503
    return jsonify(metadata), status
