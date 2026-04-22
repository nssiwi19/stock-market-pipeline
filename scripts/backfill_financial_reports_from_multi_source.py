"""
Backfill financial_reports from multiple sources and measure fill-rate gain on pilot tickers.

Sources:
1) cafef_requests            -> existing extractor logic
2) cafef_cloudscraper        -> fallback transport/parser on Cafef pages
3) vietstock_financeinfo     -> Vietstock financeinfo endpoint (BCTC summary rows)
4) vietstock_bctc_documents  -> Vietstock BCTC document endpoint (PDF/ZIP/XLS links)
"""

from __future__ import annotations

import argparse
import csv
import difflib
import io
import json
import math
import os
import re
import sys
import time
import traceback
import unicodedata
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import cloudscraper
import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2 import OperationalError
from psycopg2 import sql
try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None
try:
    import pytesseract  # type: ignore
except Exception:  # pragma: no cover
    pytesseract = None
try:
    import pypdfium2 as pdfium  # type: ignore
except Exception:  # pragma: no cover
    pdfium = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import get_supabase_client
from etl import extract_financials as ef


TARGET_COLUMNS = [
    "revenue",
    "cogs",
    "gross_profit",
    "financial_income",
    "financial_expense",
    "interest_expense",
    "selling_expense",
    "general_admin_expense",
    "operating_profit",
    "other_income",
    "other_expense",
    "profit_before_tax",
    "profit_after_tax",
    "parent_profit_after_tax",
    "minority_profit",
    "depreciation_amortization",
    "ebit",
    "ebitda",
    "eps",
    "cash_and_cash_equivalents",
    "short_term_investments",
    "short_term_receivables",
    "inventory",
    "other_current_assets",
    "total_current_assets",
    "long_term_receivables",
    "fixed_assets",
    "investment_properties",
    "long_term_assets",
    "total_assets",
    "short_term_debt",
    "accounts_payable",
    "short_term_liabilities",
    "total_short_term_liabilities",
    "long_term_debt",
    "total_long_term_liabilities",
    "total_liabilities",
    "owner_equity",
    "equity",
    "retained_earnings",
    "share_capital",
    "total_equity_and_liabilities",
    "cash_flow_operating",
    "cash_flow_investing",
    "cash_flow_financing",
    "net_cash_flow",
    "capex",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roa",
    "debt_to_equity",
    "current_ratio",
    "asset_turnover",
]

SOURCE_PRIORITY = ["cafef_requests", "vietstock_financeinfo", "vietstock_bctc_documents", "cafef_cloudscraper"]
SOURCE_CONFIDENCE = {
    "cafef_requests": 0.90,
    "vietstock_financeinfo": 0.88,
    "vietstock_bctc_documents": 0.80,
    "cafef_cloudscraper": 0.82,
}
TRUSTED_SOURCES = {"cafef_requests", "vietstock_financeinfo", "cafef_cloudscraper"}
EPS_MAX_ABS = 100_000.0
EPS_TRUSTED_SOURCES = {"cafef_requests", "vietstock_financeinfo", "cafef_cloudscraper"}
VIETSTOCK_TOKEN_URL = "https://finance.vietstock.vn/chi-so-nganh.htm"
VIETSTOCK_FINANCEINFO_URL = "https://finance.vietstock.vn/data/financeinfo"
VIETSTOCK_BCTC_DOC_PAGE_URL = "https://finance.vietstock.vn/tai-lieu/bao-cao-tai-chinh.htm"
VIETSTOCK_GET_RPT_TERM_URL = "https://finance.vietstock.vn/data/getrptterm"
VIETSTOCK_GET_RPT_FILE_URL = "https://finance.vietstock.vn/data/getrptfile"
VIETSTOCK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}
FOCUS_COLUMNS = [
    "financial_income",
    "operating_profit",
    "parent_profit_after_tax",
    "minority_profit",
    "depreciation_amortization",
    "cash_flow_operating",
    "cash_flow_investing",
    "cash_flow_financing",
    "capex",
]

METRIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    **{k: v[0] for k, v in ef.INCOME_METRICS.items()},
    **{k: v[0] for k, v in ef.BALANCE_METRICS.items()},
    **{k: v[0] for k, v in ef.CASHFLOW_METRICS.items()},
}
OCR_DEBUG_ENABLED = False
OCR_DEBUG_TICKERS: set[str] = set()
OCR_DEBUG_MAX_LINES = 120
OCR_DEBUG_MAX_FILES_PER_TICKER = 2
OCR_CHAR_MAP = str.maketrans(
    {
        "0": "o",
        "1": "i",
        "2": "z",
        "3": "e",
        "4": "a",
        "5": "s",
        "6": "o",
        "7": "t",
        "8": "b",
        "9": "g",
        "$": "s",
        "@": "a",
        "!": "i",
    }
)


def _get_db_uri() -> str:
    uri = os.getenv("SUPABASE_DB_URI") or os.getenv("DATABASE_URL")
    if not uri:
        raise RuntimeError("Missing SUPABASE_DB_URI or DATABASE_URL")
    return uri


def _empty_profile() -> dict[str, Any]:
    return {"rows": 0, "columns": len(TARGET_COLUMNS), "null_by_column": {}, "fill_rate_pct": 0.0}


def _connect_with_retry(db_uri: str, attempts: int = 4, base_sleep_s: float = 1.5):
    last_exc: Exception | None = None
    for i in range(1, max(attempts, 1) + 1):
        try:
            return psycopg2.connect(db_uri)
        except OperationalError as exc:
            last_exc = exc
            if i >= attempts:
                raise
            time.sleep(base_sleep_s * i)
    if last_exc:
        raise last_exc
    raise RuntimeError("Failed to connect to database")


def _ensure_metadata_columns(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE financial_reports
            ADD COLUMN IF NOT EXISTS source TEXT,
            ADD COLUMN IF NOT EXISTS confidence NUMERIC(5,4)
            """
        )
    conn.commit()


def _pick_pilot_tickers(cur, limit: int, mode: str, min_null_rate: float) -> list[str]:
    cols = FOCUS_COLUMNS if mode == "targeted" else TARGET_COLUMNS
    col_expr = " + ".join([f"CASE WHEN {c} IS NULL THEN 1 ELSE 0 END" for c in cols])
    denominator = len(cols)
    query = f"""
        SELECT ticker
        FROM financial_reports
        GROUP BY ticker
        HAVING (SUM({col_expr})::float / NULLIF(COUNT(*) * {denominator}, 0)) >= %s
        ORDER BY (SUM({col_expr})::float / NULLIF(COUNT(*) * {denominator}, 0)) DESC, ticker
        LIMIT %s
    """
    cur.execute(query, (min_null_rate, limit))
    rows = [r[0] for r in cur.fetchall() if r and r[0]]
    if rows:
        return rows

    # Fallback in case threshold filters out all tickers.
    query_no_threshold = f"""
        SELECT ticker
        FROM financial_reports
        GROUP BY ticker
        ORDER BY (SUM({col_expr})::float / NULLIF(COUNT(*) * {denominator}, 0)) DESC, ticker
        LIMIT %s
    """
    cur.execute(query_no_threshold, (limit,))
    return [r[0] for r in cur.fetchall() if r and r[0]]


def _fetch_existing_records(cur, tickers: list[str]) -> dict[tuple[str, str, str], dict[str, Any]]:
    if not tickers:
        return {}
    select_cols = ", ".join(["ticker", "report_type", "period"] + TARGET_COLUMNS)
    cur.execute(
        f"SELECT {select_cols} FROM financial_reports WHERE ticker = ANY(%s)",
        (tickers,),
    )
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in cur.fetchall():
        rec = dict(zip(["ticker", "report_type", "period"] + TARGET_COLUMNS, row))
        key = (str(rec["ticker"]), str(rec["report_type"]), str(rec["period"]))
        out[key] = rec
    return out


def _compute_record_gain(existing: dict[str, Any] | None, incoming: dict[str, Any]) -> int:
    if not existing:
        return 0
    gain = 0
    for col in TARGET_COLUMNS:
        if existing.get(col) is None and incoming.get(col) is not None:
            gain += 1
    return gain


def _count_non_null_target_fields(rec: dict[str, Any]) -> int:
    return sum(1 for col in TARGET_COLUMNS if rec.get(col) is not None)


def _is_new_key_insert_allowed(
    rec: dict[str, Any],
    *,
    min_fields: int,
    min_confidence: float,
    allowed_sources: set[str],
) -> bool:
    filled = _count_non_null_target_fields(rec)
    if filled < min_fields:
        return False
    confidence = float(rec.get("confidence") or 0.0)
    if confidence < min_confidence:
        return False
    src = str(rec.get("source") or "")
    src_parts = {s.strip() for s in src.split("+") if s.strip()}
    return bool(src_parts & allowed_sources)


def _sanitize_eps_for_record(rec: dict[str, Any]) -> str | None:
    eps = rec.get("eps")
    if eps is None:
        return None
    try:
        val = float(eps)
    except (TypeError, ValueError):
        rec["eps"] = None
        return "non_numeric"
    if not math.isfinite(val):
        rec["eps"] = None
        return "non_finite"

    src = str(rec.get("source") or "")
    src_parts = {s.strip() for s in src.split("+") if s.strip()}
    # EPS parsed from OCR/document rows is very noisy; trust only structured sources.
    if src_parts and src_parts.isdisjoint(EPS_TRUSTED_SOURCES):
        rec["eps"] = None
        return "untrusted_source"
    if abs(val) > EPS_MAX_ABS:
        rec["eps"] = None
        return "outlier_abs"
    rec["eps"] = round(val, 6)
    return None


def _calc_profile(
    cur,
    tickers: list[str],
    baseline_keys: set[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    if not tickers:
        return {"rows": 0, "columns": len(TARGET_COLUMNS), "null_by_column": {}, "fill_rate_pct": 0.0}

    select_cols = ", ".join(["ticker", "report_type", "period"] + TARGET_COLUMNS)
    cur.execute(f"SELECT {select_cols} FROM financial_reports WHERE ticker = ANY(%s)", (tickers,))
    rows = cur.fetchall()
    if baseline_keys is not None:
        rows = [
            row
            for row in rows
            if (str(row[0]), str(row[1]), str(row[2])) in baseline_keys
        ]

    total_rows = len(rows)
    if total_rows == 0:
        return {"rows": 0, "columns": len(TARGET_COLUMNS), "null_by_column": {}, "fill_rate_pct": 0.0}

    null_by_col = {c: 0 for c in TARGET_COLUMNS}
    for row in rows:
        for idx, col in enumerate(TARGET_COLUMNS, start=3):
            if row[idx] is None:
                null_by_col[col] += 1
    total_cells = total_rows * len(TARGET_COLUMNS)
    total_null = sum(null_by_col.values())
    fill_rate = (1.0 - total_null / total_cells) * 100.0 if total_cells else 0.0
    return {
        "rows": total_rows,
        "columns": len(TARGET_COLUMNS),
        "null_by_column": null_by_col,
        "fill_rate_pct": round(fill_rate, 2),
    }


def _build_record_from_rows(ticker: str, rows_inc, rows_bs, rows_cf) -> list[dict[str, Any]]:
    current_year = datetime.now().year
    out: list[dict[str, Any]] = []
    if not rows_inc:
        return out
    year_map = ef._build_column_year_map(rows_inc, max_cols=4, current_year=current_year)
    for col_idx in range(1, 5):
        period_str = ef._extract_period(col_idx, current_year - col_idx, year_map=year_map)
        record: dict[str, Any] = {
            "ticker": ticker,
            "report_type": "yearly",
            "period": period_str,
        }
        for field, (keywords, scale) in ef.INCOME_METRICS.items():
            record[field] = ef._extract_row_value(rows_inc, keywords, col_idx, scale)
        for field, (keywords, scale) in ef.BALANCE_METRICS.items():
            record[field] = ef._extract_row_value(rows_bs, keywords, col_idx, scale) if rows_bs else None
        for field, (keywords, scale) in ef.CASHFLOW_METRICS.items():
            record[field] = ef._extract_row_value(rows_cf, keywords, col_idx, scale) if rows_cf else None

        record["equity"] = record.get("owner_equity")
        record["ebit"] = ef._safe_add(record.get("operating_profit"), record.get("interest_expense"))
        record["ebitda"] = ef._safe_add(record.get("ebit"), record.get("depreciation_amortization"))
        record["gross_margin"] = ef._safe_div(record.get("gross_profit"), record.get("revenue"))
        record["operating_margin"] = ef._safe_div(record.get("operating_profit"), record.get("revenue"))
        record["net_margin"] = ef._safe_div(record.get("profit_after_tax"), record.get("revenue"))
        record["roe"] = ef._safe_div(record.get("profit_after_tax"), record.get("owner_equity"))
        record["roa"] = ef._safe_div(record.get("profit_after_tax"), record.get("total_assets"))
        record["debt_to_equity"] = ef._safe_div(record.get("total_liabilities"), record.get("owner_equity"))
        record["current_ratio"] = ef._safe_div(record.get("total_current_assets"), record.get("total_short_term_liabilities"))
        record["asset_turnover"] = ef._safe_div(record.get("revenue"), record.get("total_assets"))

        if record.get("revenue") is not None or record.get("total_assets") is not None:
            out.append(record)
    return out


def _fetch_rows_cloudscraper(scraper, url: str):
    response = scraper.get(url, headers=ef.HEADERS, timeout=20)
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    table = ef._find_financial_table(soup)
    if not table:
        return []
    return table.find_all("tr")


def fetch_from_cafef_cloudscraper(ticker: str) -> list[dict[str, Any]]:
    scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
    current_year = datetime.now().year
    rows_inc = _fetch_rows_cloudscraper(scraper, ef.CAFEF_INCSTA_URL.format(ticker=ticker, year=current_year))
    if not rows_inc:
        return []
    rows_bs = _fetch_rows_cloudscraper(scraper, ef.CAFEF_BSHEET_URL.format(ticker=ticker, year=current_year))
    rows_cf = _fetch_rows_cloudscraper(scraper, ef.CAFEF_CFLOW_URL.format(ticker=ticker, year=current_year))
    return _build_record_from_rows(ticker, rows_inc, rows_bs, rows_cf)


def _norm_text(text: str | None) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.lower().split())


def _vietstock_unit_to_scale(unit_code: str | None) -> float:
    unit = (unit_code or "").strip().upper()
    # financial_reports monetary fields are stored in "ty dong" (billion VND)
    if unit in {"HN", "NGHIN", "THOUSAND"}:
        return 1_000_000.0
    if unit in {"TR", "TRIEU", "MILLION"}:
        return 1_000.0
    return 1.0


def _pick_component_rows(components: dict[str, Any], aliases: tuple[str, ...]) -> list[dict[str, Any]]:
    for key, rows in components.items():
        nk = _norm_text(key)
        if any(alias in nk for alias in aliases) and isinstance(rows, list):
            return rows
    return []


def _extract_value_from_component(
    rows: list[dict[str, Any]],
    keywords: tuple[str, ...],
    value_key: str,
    divisor: float = 1.0,
) -> float | None:
    if not rows:
        return None
    for row in rows:
        name = _norm_text(row.get("Name"))
        name_en = _norm_text(row.get("NameEn"))
        hay = f"{name} {name_en}".strip()
        if not hay:
            continue
        if not any(k in hay for k in keywords):
            continue
        raw = row.get(value_key)
        if raw is None:
            return None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        if divisor > 0:
            value = value / divisor
        return round(value, 6)
    return None


def _extract_vietstock_ratio(ratio_rows: list[dict[str, Any]], value_key: str, keywords: tuple[str, ...]) -> float | None:
    val = _extract_value_from_component(ratio_rows, keywords, value_key, divisor=1.0)
    if val is None:
        return None
    return val / 100.0 if val > 1 else val


def fetch_from_vietstock_financeinfo(ticker: str) -> list[dict[str, Any]]:
    session = requests.Session()
    token_resp = session.get(VIETSTOCK_TOKEN_URL, headers=VIETSTOCK_HEADERS, timeout=25)
    token_resp.raise_for_status()
    soup = BeautifulSoup(token_resp.text, "html.parser")
    token_node = soup.select_one('input[name="__RequestVerificationToken"]')
    if not token_node or not token_node.get("value"):
        return []
    token = token_node.get("value")

    payload = {
        "Code": ticker,
        "Page": 1,
        "PageSize": 50,
        "ReportTermType": 1,  # annual
        "ReportType": "BCTQ",  # follows front-end behavior for annual tab
        "Unit": 1_000_000,
        "__RequestVerificationToken": token,
    }
    headers = dict(VIETSTOCK_HEADERS)
    headers["Referer"] = VIETSTOCK_TOKEN_URL
    r = session.post(VIETSTOCK_FINANCEINFO_URL, data=payload, headers=headers, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) < 2:
        return []
    periods = data[0] if isinstance(data[0], list) else []
    components = data[1] if isinstance(data[1], dict) else {}
    if not periods or not components:
        return []

    inc_rows = _pick_component_rows(components, ("income statement", "ket qua kinh doanh"))
    bs_rows = _pick_component_rows(components, ("balance sheet", "can doi ke toan"))
    ratio_rows = _pick_component_rows(components, ("ratio", "chi so tai chinh"))
    if not inc_rows and not bs_rows:
        return []

    out: list[dict[str, Any]] = []
    max_cols = min(4, len(periods))
    for idx in range(max_cols):
        p = periods[idx] if idx < len(periods) else {}
        value_key = f"Value{idx + 1}"
        year = p.get("YearPeriod")
        if year is None:
            continue
        period = str(year)
        unit_code = p.get("United")
        scale_to_bn = _vietstock_unit_to_scale(unit_code)
        rec: dict[str, Any] = {"ticker": ticker, "report_type": "yearly", "period": period}

        for field, (keywords, _) in ef.INCOME_METRICS.items():
            rec[field] = _extract_value_from_component(inc_rows, keywords, value_key, divisor=scale_to_bn)
        for field, (keywords, _) in ef.BALANCE_METRICS.items():
            rec[field] = _extract_value_from_component(bs_rows, keywords, value_key, divisor=scale_to_bn)

        rec["gross_margin"] = _extract_vietstock_ratio(ratio_rows, value_key, ("gross margin", "bien loi nhuan gop"))
        rec["operating_margin"] = _extract_vietstock_ratio(ratio_rows, value_key, ("operating margin", "bien loi nhuan hoat dong"))
        rec["net_margin"] = _extract_vietstock_ratio(ratio_rows, value_key, ("net margin", "bien loi nhuan rong"))
        rec["roe"] = _extract_vietstock_ratio(ratio_rows, value_key, ("roe",))
        rec["roa"] = _extract_vietstock_ratio(ratio_rows, value_key, ("roa",))
        rec["debt_to_equity"] = _extract_value_from_component(
            ratio_rows,
            ("debt/equity", "no/von chu so huu"),
            value_key,
            divisor=1.0,
        )
        rec["current_ratio"] = _extract_value_from_component(
            ratio_rows,
            ("current ratio", "he so thanh toan hien hanh"),
            value_key,
            divisor=1.0,
        )
        rec["asset_turnover"] = _extract_value_from_component(
            ratio_rows,
            ("asset turnover", "vong quay tai san"),
            value_key,
            divisor=1.0,
        )

        rec["equity"] = rec.get("owner_equity")
        rec["ebit"] = ef._safe_add(rec.get("operating_profit"), rec.get("interest_expense"))
        rec["ebitda"] = ef._safe_add(rec.get("ebit"), rec.get("depreciation_amortization"))
        rec["gross_margin"] = rec.get("gross_margin") or ef._safe_div(rec.get("gross_profit"), rec.get("revenue"))
        rec["operating_margin"] = rec.get("operating_margin") or ef._safe_div(rec.get("operating_profit"), rec.get("revenue"))
        rec["net_margin"] = rec.get("net_margin") or ef._safe_div(rec.get("profit_after_tax"), rec.get("revenue"))
        rec["roe"] = rec.get("roe") or ef._safe_div(rec.get("profit_after_tax"), rec.get("owner_equity"))
        rec["roa"] = rec.get("roa") or ef._safe_div(rec.get("profit_after_tax"), rec.get("total_assets"))
        rec["debt_to_equity"] = rec.get("debt_to_equity") or ef._safe_div(rec.get("total_liabilities"), rec.get("owner_equity"))
        rec["current_ratio"] = rec.get("current_ratio") or ef._safe_div(
            rec.get("total_current_assets"), rec.get("total_short_term_liabilities")
        )
        rec["asset_turnover"] = rec.get("asset_turnover") or ef._safe_div(rec.get("revenue"), rec.get("total_assets"))

        if rec.get("revenue") is not None or rec.get("total_assets") is not None:
            out.append(rec)
    return out


def fetch_vietstock_bctc_document_links(
    ticker: str,
    top_terms: int = 10,
    page_size: int = 20,
) -> list[dict[str, Any]]:
    """
    Fetch BCTC document links from Vietstock document endpoint.
    This source currently provides file metadata (pdf/zip/xls links), not normalized numeric rows.
    """
    session = requests.Session()
    token_resp = session.get(VIETSTOCK_BCTC_DOC_PAGE_URL, headers=VIETSTOCK_HEADERS, timeout=25)
    token_resp.raise_for_status()
    soup = BeautifulSoup(token_resp.text, "html.parser")
    token_node = soup.select_one('input[name="__RequestVerificationToken"]')
    if not token_node or not token_node.get("value"):
        return []
    token = token_node.get("value")
    headers = dict(VIETSTOCK_HEADERS)
    headers["Referer"] = VIETSTOCK_BCTC_DOC_PAGE_URL

    terms = (
        session.post(
            VIETSTOCK_GET_RPT_TERM_URL,
            data={"documentTypeID": 1, "top": int(max(top_terms, 1))},
            headers=headers,
            timeout=25,
        ).json()
        or []
    )
    out: list[dict[str, Any]] = []
    for term in terms:
        payload = {
            "stockCode": ticker,
            "documentTypeID": 1,
            "reportTermID": term.get("ReportTermID"),
            "yearPeriod": term.get("YearPeriod"),
            "exchangeID": "0",
            "orderBy": "2",
            "orderDir": "2",
            "page": "1",
            "pageSize": str(max(page_size, 1)),
            "__RequestVerificationToken": token,
        }
        rows = session.post(VIETSTOCK_GET_RPT_FILE_URL, data=payload, headers=headers, timeout=25).json() or []
        for row in rows:
            out.append(
                {
                    "ticker": ticker,
                    "year_period": term.get("YearPeriod"),
                    "report_term_id": term.get("ReportTermID"),
                    "file_ext": str(row.get("FileExt") or "").strip().lower(),
                    "title": row.get("Title"),
                    "url": row.get("Url"),
                }
            )
    return out


def _inspect_zip_document(url: str, timeout: int = 25) -> dict[str, Any]:
    """
    Inspect zip content quickly to detect if there are machine-readable files.
    """
    try:
        resp = requests.get(url, headers={"User-Agent": VIETSTOCK_HEADERS["User-Agent"]}, timeout=timeout)
        if resp.status_code != 200:
            return {"zip_inspected": False, "zip_member_count": 0, "zip_has_excel": False, "zip_has_pdf": False}
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        names = [n.lower() for n in zf.namelist()]
        has_excel = any(n.endswith((".xls", ".xlsx", ".xlsm", ".csv")) for n in names)
        has_pdf = any(n.endswith(".pdf") for n in names)
        return {
            "zip_inspected": True,
            "zip_member_count": len(names),
            "zip_has_excel": has_excel,
            "zip_has_pdf": has_pdf,
        }
    except Exception:
        return {"zip_inspected": False, "zip_member_count": 0, "zip_has_excel": False, "zip_has_pdf": False}


def _enrich_document_inventory(doc_files: list[dict[str, Any]], max_zip_probe: int) -> list[dict[str, Any]]:
    out = []
    probed = 0
    for row in doc_files:
        rec = dict(row)
        ext = str(rec.get("file_ext") or "").lower()
        rec["zip_inspected"] = False
        rec["zip_member_count"] = 0
        rec["zip_has_excel"] = False
        rec["zip_has_pdf"] = False
        if ext == ".zip" and probed < max_zip_probe:
            meta = _inspect_zip_document(str(rec.get("url") or ""))
            rec.update(meta)
            probed += 1
        out.append(rec)
    return out


def _parse_vn_number_loose(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if abs(v) < 1e-12:
            return 0.0
        return v
    text = str(value).strip()
    if not text or text in {"-", "--", "N/A", "n/a"}:
        return None
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    if text.startswith("-"):
        negative = True
        text = text[1:]
    text = text.replace(" ", "").replace(".", "").replace(",", "")
    if not text or not re.search(r"\d", text):
        return None
    try:
        v = float(text)
        return -v if negative else v
    except ValueError:
        return None


def _detect_scale_to_bn_from_df(df: pd.DataFrame) -> float:
    scan_text = []
    row_limit = min(len(df.index), 20)
    col_limit = min(len(df.columns), 8)
    for r in range(row_limit):
        for c in range(col_limit):
            v = df.iat[r, c]
            if v is None:
                continue
            scan_text.append(_norm_text(str(v)))
    hay = " ".join(scan_text)
    if "ty dong" in hay or "tỷ đồng" in hay:
        return 1.0
    if "trieu dong" in hay or "triệu đồng" in hay:
        return 1_000.0
    if "nghin dong" in hay or "ngàn đồng" in hay:
        return 1_000_000.0
    if "dong" in hay or "đồng" in hay:
        return 1_000_000_000.0
    # Most financial spreadsheets are already in million or billion units; default to million->bn.
    return 1_000.0


def _extract_year_columns_from_df(df: pd.DataFrame) -> dict[int, int]:
    out: dict[int, int] = {}
    row_limit = min(len(df.index), 14)
    for c in range(len(df.columns)):
        year_found: int | None = None
        for r in range(row_limit):
            cell = df.iat[r, c]
            if cell is None:
                continue
            m = re.search(r"(20\d{2})", str(cell))
            if not m:
                continue
            try:
                year = int(m.group(1))
            except ValueError:
                continue
            if 2000 <= year <= datetime.now().year + 1:
                year_found = year
                break
        if year_found is not None:
            out[c] = year_found
    return out


def _find_metric_field(label_text: str) -> str | None:
    nl = _norm_text(label_text)
    nl = nl.translate(OCR_CHAR_MAP)
    nl = re.sub(r"[^a-z0-9\s]+", " ", nl)
    nl = " ".join(nl.split())
    if not nl:
        return None
    nl_skel = re.sub(r"[aeiouy\s]", "", nl)

    for field, keywords in METRIC_KEYWORDS.items():
        for k in keywords:
            kw = _norm_text(k).translate(OCR_CHAR_MAP)
            kw = re.sub(r"[^a-z0-9\s]+", " ", kw)
            kw = " ".join(kw.split())
            if not kw:
                continue
            if kw in nl:
                return field
            # OCR-tolerant fallback: compare consonant skeleton and fuzzy score.
            kw_skel = re.sub(r"[aeiouy\s]", "", kw)
            if len(kw_skel) >= 8 and kw_skel in nl_skel:
                return field
            if len(kw) >= 12:
                ratio = difflib.SequenceMatcher(None, nl[: max(len(kw) + 10, 24)], kw).ratio()
                if ratio >= 0.78:
                    return field
    return None


def _records_from_dataframe(ticker: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    year_cols = _extract_year_columns_from_df(df)
    if not year_cols:
        return []
    scale_to_bn = _detect_scale_to_bn_from_df(df)
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}

    for r in range(len(df.index)):
        row_values = [df.iat[r, c] for c in range(len(df.columns))]
        label = None
        for c in range(min(4, len(df.columns))):
            v = row_values[c]
            if isinstance(v, str) and _norm_text(v):
                label = v
                break
        if not label:
            continue
        field = _find_metric_field(str(label))
        if not field:
            continue
        for c, year in year_cols.items():
            if c >= len(row_values):
                continue
            raw = _parse_vn_number_loose(row_values[c])
            if raw is None:
                continue
            rec_key = (ticker, "yearly", str(year))
            rec = by_key.setdefault(
                rec_key,
                {"ticker": ticker, "report_type": "yearly", "period": str(year)},
            )
            if rec.get(field) is None:
                rec[field] = round(raw / scale_to_bn, 6) if field != "eps" else round(raw, 6)

    out: list[dict[str, Any]] = []
    for rec in by_key.values():
        rec["equity"] = rec.get("owner_equity")
        rec["ebit"] = ef._safe_add(rec.get("operating_profit"), rec.get("interest_expense"))
        rec["ebitda"] = ef._safe_add(rec.get("ebit"), rec.get("depreciation_amortization"))
        rec["gross_margin"] = ef._safe_div(rec.get("gross_profit"), rec.get("revenue"))
        rec["operating_margin"] = ef._safe_div(rec.get("operating_profit"), rec.get("revenue"))
        rec["net_margin"] = ef._safe_div(rec.get("profit_after_tax"), rec.get("revenue"))
        rec["roe"] = ef._safe_div(rec.get("profit_after_tax"), rec.get("owner_equity"))
        rec["roa"] = ef._safe_div(rec.get("profit_after_tax"), rec.get("total_assets"))
        rec["debt_to_equity"] = ef._safe_div(rec.get("total_liabilities"), rec.get("owner_equity"))
        rec["current_ratio"] = ef._safe_div(rec.get("total_current_assets"), rec.get("total_short_term_liabilities"))
        rec["asset_turnover"] = ef._safe_div(rec.get("revenue"), rec.get("total_assets"))
        # Allow records where any parsed metric is present; merge logic will still enforce gain.
        has_metric = any(rec.get(c) is not None for c in TARGET_COLUMNS)
        if has_metric:
            out.append(rec)
    return out


def _load_dataframes_from_bytes(content: bytes, ext: str) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    ext = ext.lower()
    if ext == ".csv":
        for encoding in ("utf-8-sig", "utf-8", "cp1258", "latin1"):
            try:
                text = content.decode(encoding, errors="strict")
                df = pd.read_csv(io.StringIO(text), header=None, dtype=object)
                frames.append(df)
                break
            except Exception:
                continue
        return frames
    try:
        excel = pd.ExcelFile(io.BytesIO(content))
    except Exception:
        return frames
    for sheet in excel.sheet_names[:8]:
        try:
            df = excel.parse(sheet_name=sheet, header=None, dtype=object)
            frames.append(df)
        except Exception:
            continue
    return frames


def _detect_scale_to_bn_from_text(text: str) -> float:
    nt = _norm_text(text)
    if "ty dong" in nt or "ti dong" in nt:
        return 1.0
    if "trieu dong" in nt:
        return 1_000.0
    if "nghin dong" in nt or "ngan dong" in nt:
        return 1_000_000.0
    if "dong" in nt:
        return 1_000_000_000.0
    return 1_000.0


def _extract_candidate_years_from_text(text: str) -> list[int]:
    years = sorted({int(x) for x in re.findall(r"\b(20\d{2})\b", text) if 2000 <= int(x) <= datetime.now().year + 1}, reverse=True)
    return years[:4]


def _extract_numbers_from_line(line: str) -> list[float]:
    nums: list[float] = []
    # Vietnamese-style grouped numbers, optional sign/parentheses.
    seen: set[str] = set()
    for m in re.finditer(r"\(?-?\d{1,3}(?:[.,]\d{3}){1,6}\)?", line):
        token = m.group(0)
        if token in seen:
            continue
        seen.add(token)
        val = _parse_vn_number_loose(token)
        if val is not None:
            nums.append(val)
    # OCR fallback: long plain digit runs (no thousand separator).
    for m in re.finditer(r"\(?-?\d{6,}\)?", line):
        token = m.group(0)
        if token in seen:
            continue
        seen.add(token)
        val = _parse_vn_number_loose(token)
        if val is not None:
            nums.append(val)
    return nums


def _records_from_pdf_text(ticker: str, text: str) -> list[dict[str, Any]]:
    if not text:
        return []
    years = _extract_candidate_years_from_text(text)
    if not years:
        return []
    scale_to_bn = _detect_scale_to_bn_from_text(text)
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}

    for raw_line in text.splitlines():
        line = " ".join(raw_line.split())
        if len(line) < 6:
            continue
        field = _find_metric_field(line)
        if not field:
            continue
        values = _extract_numbers_from_line(line)
        if not values:
            continue
        usable = values[: len(years)]
        for idx, raw in enumerate(usable):
            year = years[idx]
            rec_key = (ticker, "yearly", str(year))
            rec = by_key.setdefault(
                rec_key,
                {"ticker": ticker, "report_type": "yearly", "period": str(year)},
            )
            if rec.get(field) is None:
                rec[field] = round(raw / scale_to_bn, 6) if field != "eps" else round(raw, 6)

    out: list[dict[str, Any]] = []
    for rec in by_key.values():
        rec["equity"] = rec.get("owner_equity")
        rec["ebit"] = ef._safe_add(rec.get("operating_profit"), rec.get("interest_expense"))
        rec["ebitda"] = ef._safe_add(rec.get("ebit"), rec.get("depreciation_amortization"))
        rec["gross_margin"] = ef._safe_div(rec.get("gross_profit"), rec.get("revenue"))
        rec["operating_margin"] = ef._safe_div(rec.get("operating_profit"), rec.get("revenue"))
        rec["net_margin"] = ef._safe_div(rec.get("profit_after_tax"), rec.get("revenue"))
        rec["roe"] = ef._safe_div(rec.get("profit_after_tax"), rec.get("owner_equity"))
        rec["roa"] = ef._safe_div(rec.get("profit_after_tax"), rec.get("total_assets"))
        rec["debt_to_equity"] = ef._safe_div(rec.get("total_liabilities"), rec.get("owner_equity"))
        rec["current_ratio"] = ef._safe_div(rec.get("total_current_assets"), rec.get("total_short_term_liabilities"))
        rec["asset_turnover"] = ef._safe_div(rec.get("revenue"), rec.get("total_assets"))
        if any(rec.get(c) is not None for c in TARGET_COLUMNS):
            out.append(rec)
    return out


def _ocr_debug_allowed_for_ticker(ticker: str) -> bool:
    if not OCR_DEBUG_ENABLED:
        return False
    if not OCR_DEBUG_TICKERS:
        return True
    return ticker.upper() in OCR_DEBUG_TICKERS


def _write_ocr_debug(
    ticker: str,
    source_tag: str,
    url: str,
    text: str,
    lines: list[str],
    years: list[int],
    scale_to_bn: float,
) -> None:
    if not _ocr_debug_allowed_for_ticker(ticker):
        return
    out_dir = PROJECT_ROOT / "scripts" / "output" / "ocr_debug"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    safe_ticker = re.sub(r"[^A-Za-z0-9_-]+", "_", ticker.upper())
    base = out_dir / f"{safe_ticker}_{source_tag}_{stamp}"

    sample_lines = []
    for ln in lines[: max(OCR_DEBUG_MAX_LINES, 10)]:
        nums = _extract_numbers_from_line(ln)
        field = _find_metric_field(ln)
        sample_lines.append(
            {
                "line": ln,
                "matched_field": field,
                "numbers_found": nums[:8],
            }
        )
    meta = {
        "ticker": ticker.upper(),
        "source_tag": source_tag,
        "url": url,
        "char_count": len(text),
        "line_count": len(lines),
        "years_detected": years,
        "scale_to_bn": scale_to_bn,
        "sampled_lines": len(sample_lines),
    }
    (base.with_suffix(".meta.json")).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (base.with_suffix(".lines.json")).write_text(json.dumps(sample_lines, ensure_ascii=False, indent=2), encoding="utf-8")
    (base.with_suffix(".txt")).write_text(text, encoding="utf-8", errors="ignore")


def _write_ocr_debug_event(ticker: str, source_tag: str, url: str, event: dict[str, Any]) -> None:
    if not _ocr_debug_allowed_for_ticker(ticker):
        return
    out_dir = PROJECT_ROOT / "scripts" / "output" / "ocr_debug"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    safe_ticker = re.sub(r"[^A-Za-z0-9_-]+", "_", ticker.upper())
    base = out_dir / f"{safe_ticker}_{source_tag}_{stamp}"
    payload = {"ticker": ticker.upper(), "source_tag": source_tag, "url": url, **event}
    (base.with_suffix(".event.json")).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_ticker_exception_debug(ticker: str, exc: Exception, phase: str) -> None:
    out_dir = PROJECT_ROOT / "scripts" / "output" / "ocr_debug"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    safe_ticker = re.sub(r"[^A-Za-z0-9_-]+", "_", str(ticker).upper())
    payload = {
        "ticker": str(ticker).upper(),
        "phase": phase,
        "exception_type": type(exc).__name__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
    }
    file_path = out_dir / f"{safe_ticker}_ticker_exception_{stamp}.json"
    file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _dataframes_from_pdf_bytes(content: bytes, max_pages: int = 25) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    if pdfplumber is None:
        return frames
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages[:max_pages]:
                try:
                    tables = page.extract_tables() or []
                except Exception:
                    continue
                for tbl in tables:
                    if not tbl or len(tbl) < 2:
                        continue
                    max_cols = max((len(r) for r in tbl if r), default=0)
                    if max_cols < 2:
                        continue
                    norm_rows: list[list[Any]] = []
                    for r in tbl:
                        row = list(r) if isinstance(r, (list, tuple)) else [r]
                        if len(row) < max_cols:
                            row.extend([None] * (max_cols - len(row)))
                        norm_rows.append(row)
                    df = pd.DataFrame(norm_rows, dtype=object)
                    if not df.empty:
                        frames.append(df)
    except Exception:
        return frames
    return frames


def _resolve_tesseract_cmd() -> str | None:
    env_cmd = os.getenv("TESSERACT_CMD")
    if env_cmd and Path(env_cmd).exists():
        return env_cmd
    # Common default install locations on Windows.
    candidates = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    ]
    for p in candidates:
        if Path(p).exists():
            return p
    return None


def _ocr_text_from_pdf_bytes(content: bytes, max_pages: int = 12, dpi: int = 180) -> str:
    if pytesseract is None or pdfium is None:
        return ""
    tess_cmd = _resolve_tesseract_cmd()
    if tess_cmd:
        try:
            pytesseract.pytesseract.tesseract_cmd = tess_cmd
        except Exception:
            pass

    text_parts: list[str] = []
    try:
        pdf = pdfium.PdfDocument(io.BytesIO(content))
    except Exception:
        return ""
    page_count = len(pdf)
    if page_count <= 0:
        return ""
    scale = max(float(dpi) / 72.0, 1.0)
    ocr_lang = os.getenv("OCR_LANG", "eng")
    for idx in range(min(page_count, max_pages)):
        try:
            page = pdf[idx]
            bitmap = page.render(scale=scale)
            image = bitmap.to_pil()
            try:
                txt = pytesseract.image_to_string(image, lang=ocr_lang)
            except Exception:
                txt = pytesseract.image_to_string(image)
            if txt:
                text_parts.append(txt)
        except Exception:
            continue
    return "\n".join(text_parts)


def _extract_records_from_doc_url(ticker: str, url: str, ext: str, timeout: int = 30) -> list[dict[str, Any]]:
    if not url:
        return []
    if url.startswith("/"):
        url = f"https://finance.vietstock.vn{url}"
    try:
        resp = requests.get(url, headers={"User-Agent": VIETSTOCK_HEADERS["User-Agent"]}, timeout=timeout)
        if resp.status_code != 200:
            _write_ocr_debug_event(
                ticker=ticker,
                source_tag="pdf_fetch",
                url=url,
                event={"status": "http_non_200", "http_status": resp.status_code, "ext": ext},
            )
            return []
        content = resp.content
    except Exception:
        _write_ocr_debug_event(
            ticker=ticker,
            source_tag="pdf_fetch",
            url=url,
            event={"status": "request_exception", "ext": ext},
        )
        return []

    out: list[dict[str, Any]] = []
    ext = ext.lower()
    if ext == ".zip":
        try:
            zf = zipfile.ZipFile(io.BytesIO(content))
        except Exception:
            return []
        for name in zf.namelist():
            lower_name = name.lower()
            member_ext = os.path.splitext(lower_name)[1]
            if member_ext not in {".xls", ".xlsx", ".xlsm", ".csv"}:
                continue
            try:
                payload = zf.read(name)
            except Exception:
                continue
            for df in _load_dataframes_from_bytes(payload, member_ext):
                out.extend(_records_from_dataframe(ticker, df))
        return out
    if ext not in {".xls", ".xlsx", ".xlsm", ".csv"}:
        if ext == ".pdf" and pdfplumber is not None:
            try:
                table_records: list[dict[str, Any]] = []
                for df in _dataframes_from_pdf_bytes(content, max_pages=25):
                    table_records.extend(_records_from_dataframe(ticker, df))
                if table_records:
                    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
                    for rec in table_records:
                        key = (str(rec.get("ticker")), str(rec.get("report_type")), str(rec.get("period")))
                        base = merged.setdefault(
                            key,
                            {"ticker": rec.get("ticker"), "report_type": rec.get("report_type"), "period": rec.get("period")},
                        )
                        for col in TARGET_COLUMNS:
                            if base.get(col) is None and rec.get(col) is not None:
                                base[col] = rec.get(col)
                    return list(merged.values())
                _write_ocr_debug_event(
                    ticker=ticker,
                    source_tag="pdf_table",
                    url=url,
                    event={"status": "no_records_from_tables", "table_records_raw": len(table_records)},
                )

                text_parts = []
                with pdfplumber.open(io.BytesIO(content)) as pdf:
                    for page in pdf.pages[:25]:
                        ptext = page.extract_text() or ""
                        if ptext:
                            text_parts.append(ptext)
                plain_text = "\n".join(text_parts)
                if plain_text and _ocr_debug_allowed_for_ticker(ticker):
                    lines = [" ".join(ln.split()) for ln in plain_text.splitlines() if ln and ln.strip()]
                    _write_ocr_debug(
                        ticker=ticker,
                        source_tag="pdf_text",
                        url=url,
                        text=plain_text,
                        lines=lines,
                        years=_extract_candidate_years_from_text(plain_text),
                        scale_to_bn=_detect_scale_to_bn_from_text(plain_text),
                    )
                text_records = _records_from_pdf_text(ticker, plain_text)
                if text_records:
                    return text_records
                _write_ocr_debug_event(
                    ticker=ticker,
                    source_tag="pdf_text",
                    url=url,
                    event={
                        "status": "no_records_from_text",
                        "text_chars": len(plain_text),
                        "years_detected": _extract_candidate_years_from_text(plain_text),
                    },
                )

                # Final fallback for scanned PDFs: OCR-render pages to text.
                ocr_text = _ocr_text_from_pdf_bytes(content, max_pages=12, dpi=180)
                if ocr_text:
                    if _ocr_debug_allowed_for_ticker(ticker):
                        lines = [" ".join(ln.split()) for ln in ocr_text.splitlines() if ln and ln.strip()]
                        _write_ocr_debug(
                            ticker=ticker,
                            source_tag="pdf_ocr",
                            url=url,
                            text=ocr_text,
                            lines=lines,
                            years=_extract_candidate_years_from_text(ocr_text),
                            scale_to_bn=_detect_scale_to_bn_from_text(ocr_text),
                        )
                    ocr_records = _records_from_pdf_text(ticker, ocr_text)
                    if ocr_records:
                        return ocr_records
                    _write_ocr_debug_event(
                        ticker=ticker,
                        source_tag="pdf_ocr",
                        url=url,
                        event={
                            "status": "no_records_from_ocr",
                            "ocr_chars": len(ocr_text),
                            "years_detected": _extract_candidate_years_from_text(ocr_text),
                        },
                    )
                    return []
                _write_ocr_debug_event(
                    ticker=ticker,
                    source_tag="pdf_ocr",
                    url=url,
                    event={"status": "empty_ocr_text"},
                )
                return []
            except Exception:
                _write_ocr_debug_event(
                    ticker=ticker,
                    source_tag="pdf_parse",
                    url=url,
                    event={"status": "pdf_parse_exception"},
                )
                return []
        if ext == ".pdf" and pdfplumber is None:
            _write_ocr_debug_event(
                ticker=ticker,
                source_tag="pdf_parse",
                url=url,
                event={"status": "missing_pdfplumber"},
            )
            return []
        _write_ocr_debug_event(
            ticker=ticker,
            source_tag="doc_parse",
            url=url,
            event={"status": "unsupported_extension", "ext": ext},
        )
        return []
    for df in _load_dataframes_from_bytes(content, ext):
        out.extend(_records_from_dataframe(ticker, df))
    return out


def fetch_from_vietstock_bctc_documents(
    ticker: str,
    top_terms: int = 6,
    page_size: int = 12,
    max_zip_probe: int = 1,
    max_parse_files: int = 2,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    doc_files = fetch_vietstock_bctc_document_links(ticker, top_terms=top_terms, page_size=page_size)
    doc_files = _enrich_document_inventory(doc_files, max_zip_probe=max(max_zip_probe, 0))

    parseable_exts = {".zip", ".xls", ".xlsx", ".xlsm", ".csv", ".pdf"}
    candidates = [f for f in doc_files if str(f.get("file_ext") or "").lower() in parseable_exts]
    # Keep only high-probability files for speed:
    # - Excel/CSV direct files
    # - ZIPs that are probed and confirmed to contain Excel
    high_prob: list[dict[str, Any]] = []
    for f in candidates:
        ext = str(f.get("file_ext") or "").lower()
        if ext in {".xls", ".xlsx", ".xlsm", ".csv"}:
            high_prob.append(f)
            continue
        if ext == ".pdf":
            high_prob.append(f)
            continue
        if ext == ".zip" and bool(f.get("zip_has_excel")):
            high_prob.append(f)
    if high_prob:
        candidates = high_prob
    # Prefer ZIP first because many issuers pack spreadsheets in ZIP attachments.
    candidates.sort(key=lambda x: (0 if str(x.get("file_ext")).lower() == ".zip" else 1, -int(x.get("year_period") or 0)))
    selected = candidates[: max(max_parse_files, 0)]

    merged_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in selected:
        recs = _extract_records_from_doc_url(
            ticker=ticker,
            url=str(item.get("url") or ""),
            ext=str(item.get("file_ext") or ""),
        )
        for rec in recs:
            key = (str(rec.get("ticker")), str(rec.get("report_type")), str(rec.get("period")))
            if key not in merged_by_key:
                merged_by_key[key] = {"ticker": rec.get("ticker"), "report_type": rec.get("report_type"), "period": rec.get("period")}
            base = merged_by_key[key]
            for col in TARGET_COLUMNS:
                if base.get(col) is None and rec.get(col) is not None:
                    base[col] = rec.get(col)
    return list(merged_by_key.values()), doc_files


def _merge_source_records(
    records_by_source: dict[str, list[dict[str, Any]]],
    *,
    trusted_only: bool = True,
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    used_sources: dict[tuple[str, str, str], set[str]] = defaultdict(set)

    for source in SOURCE_PRIORITY:
        if trusted_only and source not in TRUSTED_SOURCES:
            continue
        for rec in records_by_source.get(source, []):
            key = (str(rec.get("ticker")), str(rec.get("report_type")), str(rec.get("period")))
            if key not in merged:
                merged[key] = {
                    "ticker": rec.get("ticker"),
                    "report_type": rec.get("report_type"),
                    "period": rec.get("period"),
                }
            base = merged[key]
            for col in TARGET_COLUMNS:
                if base.get(col) is None and rec.get(col) is not None:
                    base[col] = rec.get(col)
                    used_sources[key].add(source)

    out = []
    for key, rec in merged.items():
        srcs = sorted(list(used_sources.get(key, set())), key=lambda s: SOURCE_PRIORITY.index(s))
        if not srcs:
            continue
        source_str = "+".join(srcs)
        confidence = min(SOURCE_CONFIDENCE.get(s, 0.75) for s in srcs)
        rec["source"] = source_str
        rec["confidence"] = round(confidence, 4)
        out.append(rec)
    return out


def _upsert_records(records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    supabase = get_supabase_client()
    batch_size = 100
    done = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        supabase.table("financial_reports").upsert(batch, on_conflict="ticker,report_type,period").execute()
        done += len(batch)
    return done


def _write_pilot_report(
    tickers: list[str],
    before: dict[str, Any],
    after: dict[str, Any],
    source_stats: dict[str, int],
    document_inventory: list[dict[str, Any]],
) -> dict[str, str]:
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "backfill_multisource_pilot_report.json"
    csv_path = out_dir / "backfill_multisource_pilot_column_delta.csv"
    doc_csv_path = out_dir / "backfill_multisource_pilot_document_inventory.csv"

    deltas = []
    for col in TARGET_COLUMNS:
        b = before["null_by_column"].get(col, 0)
        a = after["null_by_column"].get(col, 0)
        deltas.append(
            {
                "column_name": col,
                "null_before": b,
                "null_after": a,
                "filled_cells": max(b - a, 0),
            }
        )
    deltas.sort(key=lambda x: x["filled_cells"], reverse=True)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["column_name", "null_before", "null_after", "filled_cells"])
        writer.writeheader()
        writer.writerows(deltas)

    if document_inventory:
        doc_fields = [
            "ticker",
            "year_period",
            "report_term_id",
            "file_ext",
            "title",
            "url",
            "zip_inspected",
            "zip_member_count",
            "zip_has_excel",
            "zip_has_pdf",
        ]
        with doc_csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=doc_fields)
            writer.writeheader()
            writer.writerows(document_inventory)
    else:
        with doc_csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ticker", "year_period", "report_term_id", "file_ext", "title", "url"])

    payload = {
        "pilot_ticker_count": len(tickers),
        "pilot_tickers": tickers,
        "before": before,
        "after": after,
        "fill_rate_gain_pct_point": round(after["fill_rate_pct"] - before["fill_rate_pct"], 2),
        "source_stats": source_stats,
        "column_delta_csv": str(csv_path),
        "document_inventory_csv": str(doc_csv_path),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"json": str(json_path), "csv": str(csv_path), "doc_csv": str(doc_csv_path)}


def run_pilot(
    limit: int,
    sleep_ms: int,
    mode: str,
    min_null_rate: float,
    zip_probe_per_ticker: int,
    doc_parse_per_ticker: int,
    doc_top_terms: int,
    doc_page_size: int,
    allow_new_key_insert: bool = False,
    min_new_key_fields: int = 4,
    min_new_key_confidence: float = 0.8,
    new_key_allowed_sources: str = "vietstock_bctc_documents,vietstock_financeinfo",
    allow_ocr_source: bool = False,
    ocr_debug: bool = False,
    ocr_debug_tickers: str = "",
    ocr_debug_max_lines: int = 120,
    ocr_debug_max_files_per_ticker: int = 2,
) -> dict[str, Any]:
    global OCR_DEBUG_ENABLED, OCR_DEBUG_TICKERS, OCR_DEBUG_MAX_LINES, OCR_DEBUG_MAX_FILES_PER_TICKER
    OCR_DEBUG_ENABLED = bool(ocr_debug)
    OCR_DEBUG_TICKERS = {t.strip().upper() for t in str(ocr_debug_tickers or "").split(",") if t.strip()}
    OCR_DEBUG_MAX_LINES = max(int(ocr_debug_max_lines or 0), 10)
    OCR_DEBUG_MAX_FILES_PER_TICKER = max(int(ocr_debug_max_files_per_ticker or 0), 1)

    load_dotenv()
    db_uri = _get_db_uri()
    db_available = True
    try:
        with _connect_with_retry(db_uri, attempts=4, base_sleep_s=1.5) as conn:
            _ensure_metadata_columns(conn)
            with conn.cursor() as cur:
                tickers = _pick_pilot_tickers(cur, limit=limit, mode=mode, min_null_rate=min_null_rate)
                existing_map = _fetch_existing_records(cur, tickers)
                baseline_keys = set(existing_map.keys())
                before = _calc_profile(cur, tickers, baseline_keys=baseline_keys)
    except OperationalError:
        # For OCR debug, allow running without DB to inspect raw OCR outputs and keyword hits.
        if OCR_DEBUG_ENABLED and OCR_DEBUG_TICKERS:
            db_available = False
            tickers = sorted(list(OCR_DEBUG_TICKERS))
            existing_map = {}
            baseline_keys = set()
            before = _empty_profile()
        else:
            raise

    source_stats = {
        "cafef_requests_records": 0,
        "vietstock_financeinfo_records": 0,
        "vietstock_doc_files": 0,
        "vietstock_doc_pdf_files": 0,
        "vietstock_doc_zip_files": 0,
        "vietstock_doc_excel_files": 0,
        "vietstock_doc_zip_inspected": 0,
        "vietstock_doc_zip_with_excel": 0,
        "vietstock_bctc_document_records": 0,
        "cafef_cloudscraper_records": 0,
        "merged_records": 0,
        "records_skipped_new_key": 0,
        "records_inserted_new_key": 0,
        "records_skipped_new_key_low_quality": 0,
        "records_with_gain": 0,
        "records_skipped_no_gain": 0,
        "filled_cells_from_updates": 0,
        "filled_cells_from_new_keys": 0,
        "ticker_exceptions": 0,
        "source_exceptions": 0,
        "eps_cleaned_outlier_abs": 0,
        "eps_cleaned_untrusted_source": 0,
        "eps_cleaned_other": 0,
        "ocr_source_disabled": 0,
    }
    all_records: list[dict[str, Any]] = []
    all_doc_files: list[dict[str, Any]] = []
    allowed_sources_set = {s.strip() for s in str(new_key_allowed_sources or "").split(",") if s.strip()}
    if not allowed_sources_set:
        allowed_sources_set = {"vietstock_bctc_documents", "vietstock_financeinfo"}

    def _process_ticker(ticker: str):
        records_by_source: dict[str, list[dict[str, Any]]] = {}
        doc_files: list[dict[str, Any]] = []
        source_exc_count = 0
        rec1: list[dict[str, Any]] = []
        try:
            rec1 = ef.fetch_single_ticker_financials(ticker)
        except Exception as exc:
            source_exc_count += 1
            _write_ticker_exception_debug(ticker=ticker, exc=exc, phase="source_cafef_requests")
            _write_ocr_debug_event(
                ticker=ticker,
                source_tag="cafef_requests",
                url="",
                event={"status": "source_exception", "exception_type": type(exc).__name__, "message": str(exc)},
            )
        records_by_source["cafef_requests"] = rec1 or []
        rec_vs: list[dict[str, Any]] = []
        try:
            rec_vs = fetch_from_vietstock_financeinfo(ticker)
        except Exception as exc:
            source_exc_count += 1
            _write_ticker_exception_debug(ticker=ticker, exc=exc, phase="source_vietstock_financeinfo")
            _write_ocr_debug_event(
                ticker=ticker,
                source_tag="vietstock_financeinfo",
                url="",
                event={"status": "source_exception", "exception_type": type(exc).__name__, "message": str(exc)},
            )
        records_by_source["vietstock_financeinfo"] = rec_vs or []
        rec_doc: list[dict[str, Any]] = []
        if allow_ocr_source or _ocr_debug_allowed_for_ticker(ticker):
            try:
                rec_doc, doc_files = fetch_from_vietstock_bctc_documents(
                    ticker=ticker,
                    top_terms=max(doc_top_terms, 1),
                    page_size=max(doc_page_size, 1),
                    max_zip_probe=max(zip_probe_per_ticker, 0),
                    max_parse_files=max(
                        doc_parse_per_ticker if not _ocr_debug_allowed_for_ticker(ticker) else OCR_DEBUG_MAX_FILES_PER_TICKER,
                        0,
                    ),
                )
            except Exception as exc:
                source_exc_count += 1
                _write_ticker_exception_debug(ticker=ticker, exc=exc, phase="source_vietstock_bctc_documents")
                _write_ocr_debug_event(
                    ticker=ticker,
                    source_tag="vietstock_bctc_documents",
                    url="",
                    event={"status": "source_exception", "exception_type": type(exc).__name__, "message": str(exc)},
                )
        else:
            source_stats["ocr_source_disabled"] += 1
        records_by_source["vietstock_bctc_documents"] = rec_doc or []
        rec2 = []
        if (
            not records_by_source["cafef_requests"]
            and not records_by_source["vietstock_financeinfo"]
            and not records_by_source["vietstock_bctc_documents"]
        ):
            try:
                rec2 = fetch_from_cafef_cloudscraper(ticker)
            except Exception as exc:
                source_exc_count += 1
                _write_ticker_exception_debug(ticker=ticker, exc=exc, phase="source_cafef_cloudscraper")
                _write_ocr_debug_event(
                    ticker=ticker,
                    source_tag="cafef_cloudscraper",
                    url="",
                    event={"status": "source_exception", "exception_type": type(exc).__name__, "message": str(exc)},
                )
        records_by_source["cafef_cloudscraper"] = rec2 or []
        merged = _merge_source_records(records_by_source, trusted_only=not allow_ocr_source)
        return ticker, records_by_source, merged, doc_files, source_exc_count

    max_workers = min(8, max(len(tickers), 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_process_ticker, t): t for t in tickers}
        done = 0
        for future in as_completed(future_map):
            done += 1
            ticker = future_map[future]
            try:
                _, records_by_source, merged, doc_files, source_exc_count = future.result()
            except Exception as exc:
                source_stats["ticker_exceptions"] += 1
                _write_ticker_exception_debug(ticker=ticker, exc=exc, phase="process_ticker")
                records_by_source = {
                    "cafef_requests": [],
                    "vietstock_financeinfo": [],
                    "vietstock_bctc_documents": [],
                    "cafef_cloudscraper": [],
                }
                merged = []
                doc_files = []
                source_exc_count = 0
            source_stats["source_exceptions"] += source_exc_count
            source_stats["cafef_requests_records"] += len(records_by_source["cafef_requests"])
            source_stats["vietstock_financeinfo_records"] += len(records_by_source["vietstock_financeinfo"])
            source_stats["vietstock_bctc_document_records"] += len(records_by_source["vietstock_bctc_documents"])
            source_stats["cafef_cloudscraper_records"] += len(records_by_source["cafef_cloudscraper"])
            source_stats["vietstock_doc_files"] += len(doc_files)
            source_stats["vietstock_doc_pdf_files"] += sum(1 for f in doc_files if f.get("file_ext") == ".pdf")
            source_stats["vietstock_doc_zip_files"] += sum(1 for f in doc_files if f.get("file_ext") == ".zip")
            source_stats["vietstock_doc_excel_files"] += sum(
                1 for f in doc_files if f.get("file_ext") in {".xls", ".xlsx", ".xlsm", ".csv"}
            )
            source_stats["vietstock_doc_zip_inspected"] += sum(1 for f in doc_files if f.get("zip_inspected"))
            source_stats["vietstock_doc_zip_with_excel"] += sum(1 for f in doc_files if f.get("zip_has_excel"))
            all_doc_files.extend(doc_files)
            source_stats["merged_records"] += len(merged)
            for rec in merged:
                eps_reason = _sanitize_eps_for_record(rec)
                if eps_reason == "outlier_abs":
                    source_stats["eps_cleaned_outlier_abs"] += 1
                elif eps_reason == "untrusted_source":
                    source_stats["eps_cleaned_untrusted_source"] += 1
                elif eps_reason is not None:
                    source_stats["eps_cleaned_other"] += 1
                key = (str(rec.get("ticker")), str(rec.get("report_type")), str(rec.get("period")))
                if key not in existing_map:
                    source_stats["records_skipped_new_key"] += 1
                    if not allow_new_key_insert:
                        continue
                    if _is_new_key_insert_allowed(
                        rec,
                        min_fields=max(min_new_key_fields, 1),
                        min_confidence=max(min(float(min_new_key_confidence), 1.0), 0.0),
                        allowed_sources=allowed_sources_set,
                    ):
                        source_stats["records_inserted_new_key"] += 1
                        source_stats["filled_cells_from_new_keys"] += _count_non_null_target_fields(rec)
                        all_records.append(rec)
                    else:
                        source_stats["records_skipped_new_key_low_quality"] += 1
                    continue
                gain = _compute_record_gain(existing_map.get(key), rec)
                if gain > 0:
                    source_stats["records_with_gain"] += 1
                    source_stats["filled_cells_from_updates"] += gain
                    all_records.append(rec)
                else:
                    source_stats["records_skipped_no_gain"] += 1
            print(f"[{done}/{len(tickers)}] {ticker} -> merged={len(merged)}")
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)

    upserted = _upsert_records(all_records) if db_available else 0

    if db_available:
        try:
            with _connect_with_retry(db_uri, attempts=4, base_sleep_s=1.5) as conn:
                with conn.cursor() as cur:
                    after = _calc_profile(cur, tickers, baseline_keys=baseline_keys)
        except OperationalError:
            after = before
    else:
        after = before

    paths = _write_pilot_report(tickers, before, after, source_stats, all_doc_files)
    return {
        "success": True,
        "pilot_tickers": len(tickers),
        "records_upserted": upserted,
        "fill_rate_before_pct": before["fill_rate_pct"],
        "fill_rate_after_pct": after["fill_rate_pct"],
        "fill_rate_gain_pct_point": round(after["fill_rate_pct"] - before["fill_rate_pct"], 2),
        "source_stats": source_stats,
        "report_json": paths["json"],
        "column_delta_csv": paths["csv"],
        "document_inventory_csv": paths["doc_csv"],
        "mode": mode,
        "min_null_rate": min_null_rate,
        "zip_probe_per_ticker": zip_probe_per_ticker,
        "doc_parse_per_ticker": doc_parse_per_ticker,
        "doc_top_terms": doc_top_terms,
        "doc_page_size": doc_page_size,
        "allow_new_key_insert": bool(allow_new_key_insert),
        "min_new_key_fields": max(int(min_new_key_fields), 1),
        "min_new_key_confidence": round(max(min(float(min_new_key_confidence), 1.0), 0.0), 4),
        "new_key_allowed_sources": sorted(list(allowed_sources_set)),
        "allow_ocr_source": bool(allow_ocr_source),
        "ocr_debug": OCR_DEBUG_ENABLED,
        "ocr_debug_tickers": sorted(list(OCR_DEBUG_TICKERS)),
        "ocr_debug_max_lines": OCR_DEBUG_MAX_LINES,
        "ocr_debug_max_files_per_ticker": OCR_DEBUG_MAX_FILES_PER_TICKER,
        "db_available": db_available,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill financial_reports from multi-source for pilot run.")
    parser.add_argument("--pilot-size", type=int, default=50, help="Number of tickers in pilot batch.")
    parser.add_argument("--sleep-ms", type=int, default=200, help="Sleep between tickers in milliseconds.")
    parser.add_argument(
        "--mode",
        choices=["targeted", "broad"],
        default="targeted",
        help="targeted: prioritize tickers with high null in focus columns; broad: all target columns.",
    )
    parser.add_argument(
        "--min-null-rate",
        type=float,
        default=0.4,
        help="Minimum null-rate threshold for ticker selection.",
    )
    parser.add_argument(
        "--zip-probe-per-ticker",
        type=int,
        default=1,
        help="How many .zip documents to inspect per ticker for excel presence.",
    )
    parser.add_argument(
        "--doc-parse-per-ticker",
        type=int,
        default=1,
        help="How many parseable Vietstock document files (.zip/.xls/.xlsx/.csv) to parse per ticker.",
    )
    parser.add_argument(
        "--doc-top-terms",
        type=int,
        default=6,
        help="How many report terms to query from Vietstock document API per ticker.",
    )
    parser.add_argument(
        "--doc-page-size",
        type=int,
        default=12,
        help="How many files to query per report term from Vietstock document API.",
    )
    parser.add_argument(
        "--allow-new-key-insert",
        action="store_true",
        help="Allow inserting new (ticker, report_type, period) keys with quality guardrails.",
    )
    parser.add_argument(
        "--min-new-key-fields",
        type=int,
        default=4,
        help="Minimum non-null target fields required to insert a new key.",
    )
    parser.add_argument(
        "--min-new-key-confidence",
        type=float,
        default=0.8,
        help="Minimum confidence required to insert a new key.",
    )
    parser.add_argument(
        "--new-key-allowed-sources",
        type=str,
        default="vietstock_bctc_documents,vietstock_financeinfo",
        help="Comma-separated sources allowed for new key insertion.",
    )
    parser.add_argument(
        "--allow-ocr-source",
        action="store_true",
        help="Allow document/OCR source (vietstock_bctc_documents). Default is disabled.",
    )
    parser.add_argument(
        "--ocr-debug",
        action="store_true",
        help="Enable OCR debug dump to scripts/output/ocr_debug.",
    )
    parser.add_argument(
        "--ocr-debug-tickers",
        type=str,
        default="",
        help="Comma-separated tickers to debug OCR for. Empty means all pilot tickers when --ocr-debug is on.",
    )
    parser.add_argument(
        "--ocr-debug-max-lines",
        type=int,
        default=120,
        help="Max sampled lines in OCR debug line report.",
    )
    parser.add_argument(
        "--ocr-debug-max-files-per-ticker",
        type=int,
        default=2,
        help="Max document files to parse per debug ticker during OCR debug.",
    )
    args = parser.parse_args()

    result = run_pilot(
        limit=max(args.pilot_size, 1),
        sleep_ms=max(args.sleep_ms, 0),
        mode=args.mode,
        min_null_rate=max(min(args.min_null_rate, 1.0), 0.0),
        zip_probe_per_ticker=max(args.zip_probe_per_ticker, 0),
        doc_parse_per_ticker=max(args.doc_parse_per_ticker, 0),
        doc_top_terms=max(args.doc_top_terms, 1),
        doc_page_size=max(args.doc_page_size, 1),
        allow_new_key_insert=bool(args.allow_new_key_insert),
        min_new_key_fields=max(args.min_new_key_fields, 1),
        min_new_key_confidence=max(min(args.min_new_key_confidence, 1.0), 0.0),
        new_key_allowed_sources=args.new_key_allowed_sources,
        allow_ocr_source=bool(args.allow_ocr_source),
        ocr_debug=bool(args.ocr_debug),
        ocr_debug_tickers=args.ocr_debug_tickers,
        ocr_debug_max_lines=max(args.ocr_debug_max_lines, 10),
        ocr_debug_max_files_per_ticker=max(args.ocr_debug_max_files_per_ticker, 1),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
