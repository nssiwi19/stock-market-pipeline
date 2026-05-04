"""
Shared test fixtures for the stock-market-pipeline test suite.

Usage:
    python -m pytest tests/ -v
"""

import os
import sys
from pathlib import Path

import pytest

# Ensure project root is importable regardless of working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture()
def sample_ohlcv_record():
    """A valid OHLCV record for daily_prices tests."""
    return {
        "ticker": "VNM",
        "trading_date": "2026-04-15",
        "open_price": 75.0,
        "high_price": 78.5,
        "low_price": 74.0,
        "close_price": 77.2,
        "volume": 5_000_000,
    }


@pytest.fixture()
def sample_financial_record():
    """A realistic financial_reports record for ratio tests."""
    return {
        "ticker": "FPT",
        "report_type": "yearly",
        "period": "FY-2025",
        "revenue": 52.3,          # tỷ VND
        "gross_profit": 19.4,
        "operating_profit": 9.1,
        "profit_after_tax": 7.5,
        "total_assets": 45.0,
        "owner_equity": 22.0,
        "total_liabilities": 23.0,
        "total_current_assets": 28.0,
        "total_short_term_liabilities": 15.0,
        "interest_expense": 1.2,
        "depreciation_amortization": 3.0,
    }
