"""
Unit tests for AI agent safety guardrails (no API calls needed).
"""

import unittest

from etl.ai_agent import (
    _validate_readonly_sql,
    _extract_sql_from_model_text,
    _is_sensitive_investment_question,
    _required_columns_from_question,
    _question_mentions_banking,
)


class TestValidateReadonlySQL(unittest.TestCase):
    """Ensure only safe SELECT queries pass validation."""

    def test_simple_select(self):
        ok, sql = _validate_readonly_sql("SELECT ticker, volume FROM daily_prices LIMIT 10")
        self.assertTrue(ok)

    def test_cte_allowed(self):
        ok, sql = _validate_readonly_sql(
            "WITH latest AS (SELECT MAX(trading_date) AS d FROM daily_prices) "
            "SELECT * FROM daily_prices WHERE trading_date = (SELECT d FROM latest)"
        )
        self.assertTrue(ok)

    def test_insert_blocked(self):
        ok, reason = _validate_readonly_sql("INSERT INTO tickers (ticker) VALUES ('XXX')")
        self.assertFalse(ok)

    def test_delete_blocked(self):
        ok, reason = _validate_readonly_sql("DELETE FROM daily_prices WHERE ticker='XXX'")
        self.assertFalse(ok)

    def test_drop_blocked(self):
        ok, reason = _validate_readonly_sql("DROP TABLE tickers")
        self.assertFalse(ok)

    def test_update_blocked(self):
        ok, reason = _validate_readonly_sql("UPDATE tickers SET exchange='HOSE'")
        self.assertFalse(ok)

    def test_empty_sql_rejected(self):
        ok, reason = _validate_readonly_sql("")
        self.assertFalse(ok)

    def test_multiple_statements_rejected(self):
        ok, reason = _validate_readonly_sql("SELECT 1 FROM tickers; DROP TABLE tickers")
        self.assertFalse(ok)

    def test_system_catalog_blocked(self):
        ok, reason = _validate_readonly_sql("SELECT * FROM pg_catalog.pg_tables")
        self.assertFalse(ok)

    def test_unrelated_table_blocked(self):
        ok, reason = _validate_readonly_sql("SELECT * FROM users")
        self.assertFalse(ok)


class TestExtractSQLFromModelText(unittest.TestCase):
    """Extract SQL from various LLM output formats."""

    def test_plain_sql(self):
        result = _extract_sql_from_model_text("SELECT ticker FROM tickers")
        self.assertEqual(result, "SELECT ticker FROM tickers")

    def test_markdown_fenced(self):
        result = _extract_sql_from_model_text("```sql\nSELECT 1 FROM tickers\n```")
        self.assertEqual(result, "SELECT 1 FROM tickers")

    def test_markdown_no_lang(self):
        result = _extract_sql_from_model_text("```\nSELECT 1 FROM tickers\n```")
        self.assertEqual(result, "SELECT 1 FROM tickers")

    def test_empty_input(self):
        self.assertEqual(_extract_sql_from_model_text(""), "")
        self.assertEqual(_extract_sql_from_model_text(None), "")


class TestSensitiveQuestionDetection(unittest.TestCase):
    """Detect investment-advice questions to add cautionary disclaimers."""

    def test_buy_recommendation(self):
        self.assertTrue(_is_sensitive_investment_question("Tôi nên mua mã nào?"))

    def test_sell_signal(self):
        self.assertTrue(_is_sensitive_investment_question("Có nên bán FPT không?"))

    def test_neutral_question(self):
        self.assertFalse(_is_sensitive_investment_question("Volume VNM ngày hôm nay?"))


class TestRequiredColumns(unittest.TestCase):
    """Ensure correct metrics are enforced based on question keywords."""

    def test_volume_question(self):
        cols = _required_columns_from_question("Mã nào có khối lượng lớn nhất?")
        self.assertIn("volume", cols)

    def test_roe_question(self):
        cols = _required_columns_from_question("Top 5 ROE cao nhất")
        self.assertIn("roe", cols)

    def test_risk_question_includes_debt(self):
        cols = _required_columns_from_question("Mã nào ít rủi ro nhất?")
        self.assertIn("debt_to_equity", cols)

    def test_no_keyword_returns_empty(self):
        cols = _required_columns_from_question("Liệt kê tất cả các mã")
        self.assertEqual(cols, [])


class TestBankingDetection(unittest.TestCase):
    def test_vietnamese_keyword(self):
        self.assertTrue(_question_mentions_banking("Top ngân hàng lãi nhất"))

    def test_english_keyword(self):
        self.assertTrue(_question_mentions_banking("Best banking stocks"))

    def test_non_banking(self):
        self.assertFalse(_question_mentions_banking("Top 5 volume cao nhất"))


if __name__ == "__main__":
    unittest.main()
