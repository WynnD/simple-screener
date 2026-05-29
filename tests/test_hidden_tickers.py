import json
import tempfile
import unittest
from pathlib import Path

import app as screener_app


class HiddenTickersApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cache_dir = Path(self.tmp.name)
        self.original_cache_dir = screener_app.CACHE_DIR
        screener_app.CACHE_DIR = self.cache_dir
        screener_app._state.update({
            "results": [
                {"symbol": "ADBE", "name": "Adobe", "sector": "Software", "market_cap_b": 100.0, "gross_margin_pct": 80.0, "fcf_margin_3y_avg_pct": 30.0, "p_fcf": 12.0, "revenue_cagr_pct": 10.0, "ebit_cagr_pct": 8.0, "net_debt_ebitda": 0.5},
                {"symbol": "CRM", "name": "Salesforce", "sector": "Software", "market_cap_b": 200.0, "gross_margin_pct": 75.0, "fcf_margin_3y_avg_pct": 28.0, "p_fcf": 14.0, "revenue_cagr_pct": 9.0, "ebit_cagr_pct": 7.0, "net_debt_ebitda": 0.4},
            ],
            "excluded": [],
            "pending": [],
            "timestamp": "2026-05-28 04:32 UTC",
            "running": False,
            "progress": 0,
            "total": 0,
            "metadata": {},
            "last_loaded_mtime": 0.0,
        })
        self.client = screener_app.app.test_client()

    def tearDown(self):
        screener_app.CACHE_DIR = self.original_cache_dir
        self.tmp.cleanup()

    def test_hide_ticker_filters_it_from_status_and_persists_choice(self):
        response = self.client.post("/api/hidden/ADBE")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["hidden_symbols"], ["ADBE"])
        self.assertEqual(json.loads((self.cache_dir / "hidden_tickers.json").read_text()), ["ADBE"])

        status = self.client.get("/api/status").get_json()
        self.assertEqual([row["symbol"] for row in status["results"]], ["CRM"])
        self.assertEqual([row["symbol"] for row in status["hidden_results"]], ["ADBE"])
        self.assertEqual(status["hidden_symbols"], ["ADBE"])

    def test_unhide_ticker_readds_it_to_status_results(self):
        self.client.post("/api/hidden/adbe")

        response = self.client.delete("/api/hidden/adbe")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["hidden_symbols"], [])
        status = self.client.get("/api/status").get_json()
        self.assertEqual([row["symbol"] for row in status["results"]], ["ADBE", "CRM"])
        self.assertEqual(status["hidden_results"], [])

    def test_rejects_invalid_ticker_symbols(self):
        response = self.client.post("/api/hidden/../../etc/passwd")

        self.assertEqual(response.status_code, 400)
        self.assertFalse((self.cache_dir / "hidden_tickers.json").exists())

    def test_index_exposes_hide_and_unhide_controls(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn("toggleHidden", html)
        self.assertIn("Show Hidden", html)
        self.assertIn("hidden-tbody", html)


if __name__ == "__main__":
    unittest.main()
