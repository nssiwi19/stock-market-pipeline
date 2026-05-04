"""
Unit tests for daily price validation and dedup logic in extract_daily_prices.py.
"""

import unittest

from etl.extract_daily_prices import _is_price_logic_valid, _dedup_records


class TestPriceLogicValidation(unittest.TestCase):
    """Mirrors the DB CHECK constraint check_price_logic."""

    def _make_record(self, o=75.0, h=78.5, l=74.0, c=77.2, v=5_000_000):
        return {
            "open_price": o,
            "high_price": h,
            "low_price": l,
            "close_price": c,
            "volume": v,
        }

    def test_valid_record(self):
        self.assertTrue(_is_price_logic_valid(self._make_record()))

    def test_low_equals_high(self):
        # Flat day — still valid
        self.assertTrue(_is_price_logic_valid(self._make_record(o=50, h=50, l=50, c=50)))

    def test_low_greater_than_high(self):
        self.assertFalse(_is_price_logic_valid(self._make_record(l=80, h=70)))

    def test_open_below_low(self):
        self.assertFalse(_is_price_logic_valid(self._make_record(o=73.0, l=74.0)))

    def test_open_above_high(self):
        self.assertFalse(_is_price_logic_valid(self._make_record(o=80.0, h=78.5)))

    def test_close_below_low(self):
        self.assertFalse(_is_price_logic_valid(self._make_record(c=73.0, l=74.0)))

    def test_close_above_high(self):
        self.assertFalse(_is_price_logic_valid(self._make_record(c=80.0, h=78.5)))

    def test_negative_volume(self):
        self.assertFalse(_is_price_logic_valid(self._make_record(v=-1)))

    def test_zero_volume_is_valid(self):
        self.assertTrue(_is_price_logic_valid(self._make_record(v=0)))

    def test_missing_key_returns_false(self):
        self.assertFalse(_is_price_logic_valid({"open_price": 75.0}))

    def test_non_numeric_returns_false(self):
        self.assertFalse(_is_price_logic_valid(self._make_record(o="abc")))


class TestDedupRecords(unittest.TestCase):
    """Dedup by composite key before upsert to avoid DB conflicts."""

    def test_removes_duplicates_keeps_last(self):
        records = [
            {"ticker": "VNM", "trading_date": "2026-04-15", "close_price": 75.0},
            {"ticker": "VNM", "trading_date": "2026-04-15", "close_price": 76.0},
            {"ticker": "FPT", "trading_date": "2026-04-15", "close_price": 120.0},
        ]
        result = _dedup_records(records, ("ticker", "trading_date"))
        self.assertEqual(len(result), 2)
        # Last duplicate wins
        vnm = [r for r in result if r["ticker"] == "VNM"][0]
        self.assertEqual(vnm["close_price"], 76.0)

    def test_no_duplicates_unchanged(self):
        records = [
            {"ticker": "VNM", "trading_date": "2026-04-15"},
            {"ticker": "FPT", "trading_date": "2026-04-15"},
        ]
        result = _dedup_records(records, ("ticker", "trading_date"))
        self.assertEqual(len(result), 2)

    def test_empty_input(self):
        self.assertEqual(_dedup_records([], ("ticker", "trading_date")), [])


if __name__ == "__main__":
    unittest.main()
