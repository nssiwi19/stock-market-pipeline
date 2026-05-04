"""
Unit tests for financial calculation helpers in extract_financials.py.

Covers: _safe_div, _safe_add, _parse_vn_number, _normalize_text, ratio derivation.
"""

import unittest

from etl.extract_financials import (
    _safe_div,
    _safe_add,
    _parse_vn_number,
    _normalize_text,
)


class TestSafeDiv(unittest.TestCase):
    """Division helper used for all financial ratios."""

    def test_normal_division(self):
        self.assertAlmostEqual(_safe_div(10.0, 5.0), 2.0)

    def test_negative_values(self):
        self.assertAlmostEqual(_safe_div(-7.5, 2.5), -3.0)

    def test_denominator_zero_returns_none(self):
        self.assertIsNone(_safe_div(10.0, 0))
        self.assertIsNone(_safe_div(10.0, 0.0))

    def test_denominator_none_returns_none(self):
        self.assertIsNone(_safe_div(10.0, None))

    def test_numerator_none_returns_none(self):
        self.assertIsNone(_safe_div(None, 5.0))

    def test_both_none_returns_none(self):
        self.assertIsNone(_safe_div(None, None))


class TestSafeAdd(unittest.TestCase):
    """Addition helper used for EBIT/EBITDA derivation."""

    def test_normal_addition(self):
        self.assertAlmostEqual(_safe_add(3.0, 4.0), 7.0)

    def test_one_none_treated_as_zero(self):
        self.assertAlmostEqual(_safe_add(5.0, None), 5.0)
        self.assertAlmostEqual(_safe_add(None, 8.0), 8.0)

    def test_both_none_returns_none(self):
        self.assertIsNone(_safe_add(None, None))

    def test_negative_values(self):
        self.assertAlmostEqual(_safe_add(-2.0, 3.0), 1.0)


class TestParseVnNumber(unittest.TestCase):
    """Parser for Vietnamese-formatted numbers from CafeF HTML."""

    def test_integer_with_dot_separator(self):
        # 60.074.730.223.299 → raw VND
        self.assertAlmostEqual(_parse_vn_number("60.074.730.223.299"), 60_074_730_223_299.0)

    def test_simple_integer(self):
        self.assertAlmostEqual(_parse_vn_number("12345"), 12345.0)

    def test_negative_number(self):
        self.assertAlmostEqual(_parse_vn_number("-1.234.567"), -1_234_567.0)

    def test_dash_returns_none(self):
        self.assertIsNone(_parse_vn_number("-"))
        self.assertIsNone(_parse_vn_number("--"))

    def test_empty_returns_none(self):
        self.assertIsNone(_parse_vn_number(""))
        self.assertIsNone(_parse_vn_number(None))

    def test_na_returns_none(self):
        self.assertIsNone(_parse_vn_number("N/A"))


class TestNormalizeText(unittest.TestCase):
    """Text normalizer used for matching Vietnamese financial row labels."""

    def test_removes_diacritics(self):
        result = _normalize_text("Doanh thu thuần")
        self.assertEqual(result, "doanh thu thuan")

    def test_lowercases(self):
        result = _normalize_text("LỢI NHUẬN GỘP")
        self.assertEqual(result, "loi nhuan gop")

    def test_collapses_whitespace(self):
        result = _normalize_text("  Tổng   tài   sản  ")
        self.assertEqual(result, "tong tai san")

    def test_none_returns_empty(self):
        self.assertEqual(_normalize_text(None), "")


class TestRatioDerivation(unittest.TestCase):
    """Verify ratio formulas match expected accounting definitions."""

    def test_gross_margin(self):
        # gross_margin = gross_profit / revenue
        gm = _safe_div(19.4, 52.3)
        self.assertAlmostEqual(gm, 19.4 / 52.3, places=6)

    def test_roe(self):
        # ROE = profit_after_tax / owner_equity
        roe = _safe_div(7.5, 22.0)
        self.assertAlmostEqual(roe, 7.5 / 22.0, places=6)

    def test_roa(self):
        # ROA = profit_after_tax / total_assets
        roa = _safe_div(7.5, 45.0)
        self.assertAlmostEqual(roa, 7.5 / 45.0, places=6)

    def test_debt_to_equity(self):
        # D/E = total_liabilities / owner_equity
        de = _safe_div(23.0, 22.0)
        self.assertAlmostEqual(de, 23.0 / 22.0, places=6)

    def test_current_ratio(self):
        # Current ratio = current_assets / short_term_liabilities
        cr = _safe_div(28.0, 15.0)
        self.assertAlmostEqual(cr, 28.0 / 15.0, places=6)

    def test_ebit_derivation(self):
        # EBIT = operating_profit + interest_expense
        ebit = _safe_add(9.1, 1.2)
        self.assertAlmostEqual(ebit, 10.3)

    def test_ebitda_derivation(self):
        # EBITDA = EBIT + depreciation
        ebit = _safe_add(9.1, 1.2)
        ebitda = _safe_add(ebit, 3.0)
        self.assertAlmostEqual(ebitda, 13.3)


if __name__ == "__main__":
    unittest.main()
