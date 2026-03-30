"""Flask web app for the stock screener."""

import logging
import threading

from flask import Flask, jsonify, render_template_string

from screener import run_screen_cached, CACHE_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# In-memory state for background screening
_state = {
    "results": None,
    "pending": None,
    "timestamp": None,
    "running": False,
    "progress": 0,
    "total": 0,
}


def _load_stale_cache():
    """Load whatever results exist on disk, regardless of age."""
    import json, time
    from datetime import datetime
    cache_file = CACHE_DIR / "results.json"
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text())
            ts = datetime.fromtimestamp(cache_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            _state["results"] = data.get("results", data if isinstance(data, list) else [])
            _state["pending"] = data.get("pending", [])
            _state["timestamp"] = ts
            logger.info(f"Loaded stale cache: {len(_state['results'])} results from {ts}")
        except Exception as e:
            logger.error(f"Failed to load stale cache: {e}")


def _progress_cb(done, total):
    _state["progress"] = done
    _state["total"] = total


def _run_background():
    _state["running"] = True
    _state["progress"] = 0
    try:
        data, ts = run_screen_cached(max_age_hours=1, progress_callback=_progress_cb)
        _state["results"] = data.get("results", [])
        _state["pending"] = data.get("pending", [])
        _state["timestamp"] = ts
    except Exception as e:
        logger.error(f"Screen failed: {e}")
    finally:
        _state["running"] = False


# Load cached results immediately so the UI has data right away
_load_stale_cache()
# Then refresh in background
threading.Thread(target=_run_background, daemon=True).start()

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
  .subtitle { color: var(--muted); font-size: 0.85rem; margin-bottom: 1.5rem; }
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
let sortKey = 'p_fcf', sortAsc = true, data = [], pending = [];

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

  // Append pending rows at the bottom
  if (pending.length > 0) {
    pending.forEach(r => tbody.appendChild(renderRow(r, true)));
  }

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
      pending = s.pending || [];
      let meta = data.length + ' stocks passing';
      if (pending.length > 0) meta += ' \u00b7 ' + pending.length + ' awaiting data';
      meta += ' \u00b7 Updated ' + s.timestamp;
      document.getElementById('meta').textContent = meta;
      render();
    } else {
      document.getElementById('meta').textContent = 'No results yet. Click Refresh.';
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
    return jsonify({
        "running": _state["running"],
        "progress": _state["progress"],
        "total": _state["total"],
        "results": _state["results"],
        "pending": _state["pending"],
        "timestamp": _state["timestamp"],
    })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if _state["running"]:
        return jsonify({"ok": False, "msg": "Already running"})

    # Delete cache to force re-fetch
    from pathlib import Path
    cache_file = Path(__file__).parent / "cache" / "results.json"
    if cache_file.exists():
        cache_file.unlink()

    threading.Thread(target=_run_background, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/health")
def health():
    return "ok"
