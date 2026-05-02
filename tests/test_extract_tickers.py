import unittest

from etl.extract_tickers import _clean_nullable_text, _clean_text


class TestExtractTickersCleaning(unittest.TestCase):
    def test_clean_text_fallback_for_empty_like_values(self):
        self.assertEqual(_clean_text(None, fallback="N/A"), "N/A")
        self.assertEqual(_clean_text(""), "N/A")
        self.assertEqual(_clean_text("  none  "), "N/A")
        self.assertEqual(_clean_text("NaN"), "N/A")

    def test_clean_text_keeps_valid_text(self):
        self.assertEqual(_clean_text("  FPT  "), "FPT")
        self.assertEqual(_clean_text("HOSE"), "HOSE")

    def test_clean_nullable_text_returns_none_for_invalid_values(self):
        self.assertIsNone(_clean_nullable_text(None))
        self.assertIsNone(_clean_nullable_text("  "))
        self.assertIsNone(_clean_nullable_text("unknown"))

    def test_clean_nullable_text_keeps_valid_value(self):
        self.assertEqual(_clean_nullable_text("Technology"), "Technology")


if __name__ == "__main__":
    unittest.main()
