import unittest
from pathlib import Path

from gen_pbgr_data import calc_kr, implied_cagr_kr


class PbgrCalculationTest(unittest.TestCase):
    def test_market_implied_cagr_reprices_to_current_price(self):
        price = 108_700
        equity_100m = 2_580
        shares = 22_177_360
        dv = 8.0
        required_return = 0.10

        implied_pct = implied_cagr_kr(
            price, equity_100m, shares, dv, required_return
        )
        calc = calc_kr(
            price, equity_100m, implied_pct, shares, dv, required_return
        )

        self.assertIsNotNone(calc)
        assert calc is not None
        self.assertAlmostEqual(calc["pbgr"], 1.0, places=3)
        self.assertAlmostEqual(calc["fair_price"], price, delta=1)

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


class PbgrUiContractTest(unittest.TestCase):
    def test_market_evaluation_is_the_single_cagr_field_after_market_cap(self):
        app = Path("app.js").read_text(encoding="utf-8")
        html = Path("index.html").read_text(encoding="utf-8")

        self.assertIn('class="market-cagr-input"', app)
        self.assertIn('class="market-cagr-reset"', app)
        self.assertIn("시장 평가 초기화", app)
        self.assertIn("resolveMarketCagrKR", app)
        self.assertIn("market_cagr_overrides", app)
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
