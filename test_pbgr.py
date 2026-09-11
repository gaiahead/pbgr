import copy
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import gen_pbgr_data as generator
from gen_pbgr_data import calc_kr, date_value, implied_cagr_kr


class PbgrCalculationTest(unittest.TestCase):
    def test_month_end_base_uses_actual_elapsed_period(self):
        today = datetime(2026, 8, 5)
        expected_months = (today - datetime(2025, 12, 31)).days / (365.2425 / 12)
        self.assertAlmostEqual(date_value("2025.12", today), expected_months, places=12)

    def test_market_implied_cagr_reprices_to_current_price(self):
        price = 108_700
        equity_100m = 2_580
        shares = 22_177_360
        dv = 8.0
        required_return = 0.10

        implied_pct = implied_cagr_kr(
            price, equity_100m, shares, dv, required_return
        )
        self.assertIsNotNone(implied_pct)
        assert implied_pct is not None
        calc = calc_kr(
            price, equity_100m, implied_pct, shares, dv, required_return
        )

        self.assertIsNotNone(calc)
        assert calc is not None
        self.assertAlmostEqual(calc["pbgr"], 1.0, places=3)
        self.assertAlmostEqual(calc["fair_price"], price, delta=1)
        self.assertAlmostEqual(
            calc["equity10_100m"],
            calc["equity_now_100m"] * (1 + implied_pct / 100) ** 10,
            places=6,
        )

    def test_selected_market_cagr_drives_valuation(self):
        price = 108_700
        equity_100m = 2_580
        shares = 22_177_360
        dv = 8.0
        required_return = 0.10
        conservative_cagr_pct = 7.51
        aggressive_cagr_pct = 15.25

        conservative_calc = calc_kr(
            price, equity_100m, conservative_cagr_pct, shares, dv, required_return
        )
        aggressive_calc = calc_kr(
            price, equity_100m, aggressive_cagr_pct, shares, dv, required_return
        )

        assert conservative_calc is not None
        assert aggressive_calc is not None
        self.assertNotEqual(
            conservative_calc["fair_price"], aggressive_calc["fair_price"]
        )
        self.assertAlmostEqual(
            (1 / conservative_calc["pbgr"] - 1) * 100,
            (conservative_calc["fair_price"] / price - 1) * 100,
            delta=0.1,
        )


class PbgrMarketCoverageTest(unittest.TestCase):
    KR_TICKERS = [
        "005930", "009150", "000660", "042700", "058470", "000100",
        "035420", "357780", "064760", "079940", "093320", "108320",
        "005290", "086450", "112610", "030190", "058610", "010120",
        "298040", "267260", "006260", "001440", "475150",
    ]

    def test_config_and_generated_payload_include_sk_eternix(self):
        config = json.loads(Path("config.json").read_text(encoding="utf-8"))
        payload = json.loads(Path("pbgr_data.json").read_text(encoding="utf-8"))
        generator.validate_payload(payload, config)
        self.assertEqual(list(config["kr"]["assets"]), self.KR_TICKERS)
        self.assertEqual([asset["ticker"] for asset in payload["assets"]], self.KR_TICKERS)
        asset = payload["assets"][-1]
        self.assertEqual(asset["name"], "SK이터닉스")
        self.assertGreater(asset["price"], 0)
        self.assertGreater(asset["shares"], 0)
        self.assertTrue(asset["equity_series"])
        self.assertTrue(all(asset.get(field) is not None for field in (
            "equity_y0_100m", "valuation_cagr_pct", "market_implied_cagr_pct",
            "equity_now_100m", "pbgr", "fair_price",
        )))
        if asset["actual_equity_cagr_pct"] is None:
            self.assertAlmostEqual(
                asset["valuation_cagr_pct"], asset["market_implied_cagr_pct"], places=4
            )


class PbgrGenerationSafetyTest(unittest.TestCase):
    def setUp(self):
        self.config = json.loads(Path("config.json").read_text(encoding="utf-8"))
        self.config["kr"]["assets"] = {
            ticker: self.config["kr"]["assets"][ticker] for ticker in ("005930", "009150")
        }
        self.payload = json.loads(Path("pbgr_data.json").read_text(encoding="utf-8"))
        self.payload["assets"] = self.payload["assets"][:2]
        self.previous = {asset["ticker"]: asset for asset in self.payload["assets"]}
        self.output = Path(self.enterContext(tempfile.TemporaryDirectory())) / "pbgr_data.json"
        self.output.write_text(json.dumps(self.payload), encoding="utf-8")
        self.original_bytes = self.output.read_bytes()
        self.enterContext(patch.object(generator, "OUTPUT_PATH", self.output))
        self.enterContext(patch.object(generator, "load_config", return_value=self.config))
        self.logs = self.enterContext(redirect_stdout(io.StringIO()))
        self.price = self.enterContext(patch.object(
            generator, "get_naver_price",
            side_effect=lambda ticker: self.previous[ticker]["price"] + 1000,
        ))
        share_counts = {
            "005930": self.previous["005930"]["shares_common"],
            "005935": self.previous["005930"]["shares_preferred"],
            "009150": self.previous["009150"]["shares_common"],
        }
        self.shares = self.enterContext(patch.object(
            generator, "_fetch_listed_shares", side_effect=share_counts.get,
        ))
        self.http = self.enterContext(patch.object(
            generator, "_http_get", return_value=(
                '<div class="cop_analysis"><table><th>2025.12</th>'
                '<tr><th>BPS()</th><td>63,997</td></tr></table></div>'
            ),
        ))
        self.enterContext(patch.object(
            generator, "get_wisereport_data",
            return_value=(None, None, {}, copy.deepcopy(generator._EMPTY_ROE_HIST)),
        ))

    def assert_fallback(self, tickers):
        generator.main()
        payload = json.loads(self.output.read_text(encoding="utf-8"))
        generator.validate_payload(payload, self.config)
        for asset in payload["assets"]:
            ticker = asset["ticker"]
            if ticker in tickers:
                self.assertEqual(asset, self.previous[ticker])
                self.assertIn(f"{ticker}: fallback to complete previous asset", self.logs.getvalue())
            else:
                self.assertEqual(asset["price"], self.previous[ticker]["price"] + 1000)

    def test_tableless_naver_preserves_complete_prior_rows(self):
        self.http.return_value = "<html>Temporarily unavailable</html>"
        self.assert_fallback(self.previous)

    def test_missing_naver_shares_preserves_complete_prior_rows(self):
        self.shares.side_effect = None
        self.shares.return_value = None
        self.assert_fallback(self.previous)

    def test_per_asset_exception_falls_back_and_other_asset_updates(self):
        def fetch_price(ticker):
            if ticker == "005930":
                raise TimeoutError("Naver unavailable")
            return self.previous[ticker]["price"] + 1000

        self.price.side_effect = fetch_price
        self.assert_fallback({"005930"})

    def test_partial_preferred_shares_fall_back(self):
        self.shares.side_effect = lambda ticker: self.previous.get(ticker, {}).get("shares_common")
        self.assert_fallback({"005930"})

    def test_invalid_candidate_without_valid_predecessor_aborts_without_writing(self):
        self.http.return_value = "<html>No financials</html>"
        for assets in ([], [{**self.previous["005930"], "base_date": None}]):
            with self.subTest(assets=assets):
                self.output.write_text(json.dumps({"assets": assets}), encoding="utf-8")
                before = self.output.read_bytes()
                with self.assertRaisesRegex(RuntimeError, "no valid previous asset"):
                    generator.main()
                self.assertEqual(self.output.read_bytes(), before)

    def test_exception_without_predecessor_does_not_create_output(self):
        self.output.unlink()
        self.price.side_effect = TimeoutError("Naver unavailable")
        with self.assertRaisesRegex(RuntimeError, "no valid previous asset"):
            generator.main()
        self.assertFalse(self.output.exists())

    def test_required_output_contract_rejects_invalid_fields(self):
        positive_fields = ("price", "shares", "shares_common", "equity_y0_100m",
                           "fair_price", "pbgr", "shares_preferred")
        finite_fields = ("market_implied_cagr_pct", "equity_now_100m", "valuation_cagr_pct",
                         "required_return_pct")
        for field in positive_fields + finite_fields:
            invalid_values = [None, True, "123", float("nan"), float("inf"), -float("inf")]
            if field in positive_fields:
                invalid_values += [0, -1]
            for value in invalid_values:
                with self.subTest(field=field, value=value):
                    asset = {**self.previous["005930"], field: value}
                    with self.assertRaisesRegex(ValueError, field):
                        generator.validate_asset(asset, "005930", self.config["kr"]["assets"]["005930"])
        for value in (None, "", "2025.13", "2025.12junk", "2025-02-30"):
            with self.subTest(base_date=value):
                asset = {**self.previous["005930"], "base_date": value}
                with self.assertRaisesRegex(ValueError, "base_date"):
                    generator.validate_asset(asset, "005930", self.config["kr"]["assets"]["005930"])

    def test_share_totals_must_include_common_and_preferred(self):
        for ticker, cfg in self.config["kr"]["assets"].items():
            with self.subTest(ticker=ticker):
                asset = {**self.previous[ticker], "shares": self.previous[ticker]["shares"] + 1}
                with self.assertRaisesRegex(ValueError, "shares must equal"):
                    generator.validate_asset(asset, ticker, cfg)

    def test_optional_forecast_and_roe_fields_may_be_null(self):
        asset = {**self.previous["005930"], "equity_cagr_pct": None,
                 "actual_equity_cagr_pct": None, "roe_pct": None, "roe_ref": None}
        generator.validate_asset(asset, "005930", self.config["kr"]["assets"]["005930"])

    def test_complete_payload_rejects_missing_duplicate_and_invalid_assets(self):
        for assets in (self.payload["assets"][:1], self.payload["assets"] * 2,
                       [*self.payload["assets"][:1], {**self.previous["009150"], "price": None}]):
            with self.subTest(assets=assets):
                with self.assertRaises(ValueError):
                    generator.validate_payload({**self.payload, "assets": assets}, self.config)

    def test_final_payload_validation_runs_before_output_write(self):
        self.config["kr"]["required_return"] = float("nan")
        with self.assertRaises(ValueError):
            generator.main()
        for ticker in self.previous:
            self.assertIn(f"{ticker}: fallback to complete previous asset", self.logs.getvalue())
        self.assertEqual(self.output.read_bytes(), self.original_bytes)


class PbgrUiContractTest(unittest.TestCase):
    def test_market_evaluation_is_the_single_cagr_field_after_market_cap(self):
        app = Path("app.js").read_text(encoding="utf-8")
        html = Path("index.html").read_text(encoding="utf-8")

        self.assertIn('class="market-cagr-input"', app)
        self.assertIn('class="market-cagr-reset"', app)
        self.assertIn("시장 평가 초기화", app)
        self.assertIn("resolveMarketCagrKR", app)
        self.assertIn("market_cagr_overrides", app)
        self.assertIn(": (a.equity_y0_100m ?? null)", app)
        self.assertIn("pbgr_data.json?v=data-repair-20260911", app)
        self.assertIn("app.js?v=data-repair-20260911", html)
        self.assertIn("PBGR · 적정가 · 괴리율 = 시장 평가 자본 CAGR 기준", html)
        self.assertNotIn("PBGR · 적정가 · 괴리율 = 5년 실적 자본 CAGR 기준", html)
        self.assertNotIn("5년 실적", html)
        self.assertNotIn("3년 기대", html)
        self.assertNotIn(">자본 CAGR<", html)
        self.assertIn('<th class="group-head" colspan="3">자본총계</th>', html)
        self.assertLess(html.index("시가총액</th>"), html.index("시장 평가 ✎"))
        self.assertLess(html.index("시장 평가 ✎"), html.index("적정 시가총액</th>"))


if __name__ == "__main__":
    unittest.main()
