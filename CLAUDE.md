# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A stock screener web app that filters US equities. Uses Yahoo Finance for bulk data with FMP (Financial Modeling Prep) as fallback. Deployed on k3s via ArgoCD.

## Screening Criteria

- Market Cap > $3B
- FCF Margin % (3Y avg annual) > 15%
- Gross Profit Margin % (LTM) > 45%
- Total Revenue CAGR (3Y) > 3%
- P/FCF (LTM) between 7x - 17x
- EBIT CAGR (3Y) > 0%
- Net Debt / EBITDA (LTM) < 2.5x

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run locally
source .venv/bin/activate
FMP_API_KEY=xxx flask --app app run --port 8080

# Build & deploy
docker build -t registry.wynndrahorad.com/simple-screener:latest .
docker push registry.wynndrahorad.com/simple-screener:latest
kubectl rollout restart deployment/simple-screener -n simple-screener
```

## Architecture

Two-phase screening pipeline:

1. **Yahoo screener (server-side bulk filter)**: Queries Yahoo's equity screener API with GM>45%, ND/EBITDA<2.5x, MktCap>$3B, FCF>0, US region. Reduces ~3600 stocks to ~400 candidates in seconds. No per-ticker API calls needed.

2. **Yahoo per-ticker analysis**: For each survivor, fetches quarterly statements (TTM calculations) and annual statements (3Y CAGR, FCF margin avg). Applies remaining criteria (P/FCF range, FCF margin 3Y avg, Revenue/EBIT CAGR).

3. **FMP fallback**: When Yahoo returns empty data for a ticker, uses FMP API (250 req/day free tier). FMP key stored as k8s secret `fmp-api` in cluster (not in git).

- **`screener.py`** — Core logic. `get_candidate_tickers()` does server-side pre-filtering. `analyze_ticker_yahoo()` computes TTM from quarterly data and CAGR from annual. `analyze_ticker_fmp()` is the fallback. `run_screen()` orchestrates with ThreadPoolExecutor (6 workers).
- **`app.py`** — Flask web app. Background thread starts screening on boot. Polls via `/api/status`, refreshes via `/api/refresh`. Single-page dark-mode UI with sortable table.

## Deployment

k3s cluster via ArgoCD. Manifests in `../home-cluster/`:
- `kustomize/simple-screener/` — deployment, service, ingress, local-only middleware
- `argo/simple-screener/` — ArgoCD Application

Ingress: `https://screener.wynndrahorad.com` (HTTPS only, local-only middleware)

FMP API key: `kubectl get secret fmp-api -n simple-screener` (created manually, not in git)

## Quirks

- Full scan takes ~3 minutes (400 pre-filtered tickers), cached for 6 hours.
- Yahoo screener fields: `grossprofitmargin.lasttwelvemonths`, `netdebtebitda.lasttwelvemonths`, `leveredfreecashflow.lasttwelvemonths` — see `EQUITY_SCREENER_FIELDS` in yfinance source for full list.
- Yahoo's gross profit definition differs from some data providers (e.g., MELI shows 44.5% vs 50.68% elsewhere). This is a known data source divergence, not a bug.
- FMP free tier (250 req/day) only supports `/stable/profile` endpoint. Financial statements and ratios require paid plan. The FMP fallback code exists but is non-functional on the current free key.
- `gunicorn --workers 1` required — screening state is in-process memory.
