"""Flask web app for the stock screener."""

import logging
import threading

from flask import Flask, jsonify, render_template_string

from screener import run_screen_cached

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# In-memory state for background screening
_state = {
    "results": None,
    "timestamp": None,
    "running": False,
    "progress": 0,
    "total": 0,
}


def _progress_cb(done, total):
    _state["progress"] = done
    _state["total"] = total


def _run_background():
    _state["running"] = True
    _state["progress"] = 0
    try:
        results, ts = run_screen_cached(max_age_hours=1, progress_callback=_progress_cb)
        _state["results"] = results
        _state["timestamp"] = ts
    except Exception as e:
        logger.error(f"Screen failed: {e}")
    finally:
        _state["running"] = False


# Kick off initial screen on startup
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
  @media (max-width: 900px) {
    body { padding: 1rem; }
    .name { display: none; }
  }
</style>
</head>
<body>
  <div class="header-row">
    <h1>Stock Screener</h1>
    <button class="btn" id="refresh" onclick="refresh()">Refresh</button>
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

<script>
let sortKey = 'p_fcf', sortAsc = true, data = [];

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function render() {
  const sorted = [...data].sort((a, b) => {
    let va = a[sortKey], vb = b[sortKey];
    if (typeof va === 'string') return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
    return sortAsc ? va - vb : vb - va;
  });
  const tbody = document.getElementById('tbody');
  tbody.textContent = '';
  sorted.forEach(r => {
    const tr = document.createElement('tr');
    const cells = [
      { cls: 'sym', text: r.symbol },
      { cls: 'name', text: r.name },
      { cls: 'name', text: r.sector },
      { cls: 'num', text: r.market_cap_b.toFixed(1) },
      { cls: 'num', text: r.gross_margin_pct.toFixed(1) + '%' },
      { cls: 'num', text: r.fcf_margin_3y_avg_pct.toFixed(1) + '%' },
      { cls: 'num good', text: r.p_fcf.toFixed(1) + 'x' },
      { cls: 'num', text: r.revenue_cagr_pct.toFixed(1) + '%' },
      { cls: 'num', text: r.ebit_cagr_pct.toFixed(1) + '%' },
      { cls: 'num', text: r.net_debt_ebitda.toFixed(2) + 'x' },
    ];
    cells.forEach(c => {
      const td = document.createElement('td');
      td.className = c.cls;
      td.textContent = c.text;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

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
      document.getElementById('meta').textContent =
        data.length + ' stocks passing \u00b7 Updated ' + s.timestamp;
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

poll();
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/status")
def api_status():
    return jsonify({
        "running": _state["running"],
        "progress": _state["progress"],
        "total": _state["total"],
        "results": _state["results"],
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
