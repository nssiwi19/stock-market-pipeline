"""
Rule-based imputation v1 for financial_reports.

Scope:
- Fill only NULL cells using deterministic accounting identities.
- Keep source-of-truth behavior: existing non-null values are never overwritten.
- Write per-cell lineage to financial_reports_imputation_log.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "scripts" / "output"


def _get_db_uri() -> str:
    uri = os.getenv("SUPABASE_DB_URI") or os.getenv("DATABASE_URL")
    if not uri:
        raise RuntimeError("Missing SUPABASE_DB_URI or DATABASE_URL")
    return uri


def _ensure_imputation_log_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS financial_reports_imputation_log (
            id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
            run_id TEXT NOT NULL,
            ticker VARCHAR(10) NOT NULL,
            report_type VARCHAR(20) NOT NULL,
            period VARCHAR(20) NOT NULL,
            column_name TEXT NOT NULL,
            imputed_value TEXT,
            method TEXT NOT NULL,
            confidence NUMERIC(5,4) NOT NULL DEFAULT 1.0000,
            source_columns TEXT[] NOT NULL DEFAULT '{}',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now())
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fr_imputation_log_run_id
            ON financial_reports_imputation_log (run_id)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fr_imputation_log_ticker_period
            ON financial_reports_imputation_log (ticker, report_type, period)
        """
    )


def _null_profile(cur) -> dict[str, dict[str, float]]:
    cols = [
        "revenue",
        "cogs",
        "gross_profit",
        "operating_profit",
        "profit_after_tax",
        "total_assets",
        "total_liabilities",
        "owner_equity",
        "equity",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "roe",
        "roa",
        "debt_to_equity",
        "current_ratio",
        "asset_turnover",
        "ebit",
        "ebitda",
        "total_equity_and_liabilities",
    ]
    agg = ", ".join([f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_null" for c in cols])
    cur.execute(
        f"""
        SELECT COUNT(*) AS total_rows, {agg}
        FROM financial_reports
        """
    )
    row = cur.fetchone()
    total = int(row[0] or 0)
    out: dict[str, dict[str, float]] = {}
    for i, c in enumerate(cols, start=1):
        nulls = int(row[i] or 0)
        out[c] = {
            "null_rows": nulls,
            "total_rows": total,
            "null_rate_pct": round((nulls * 100.0 / total), 2) if total else 0.0,
        }
    return out


def _apply_rule(cur, run_id: str, rule: dict[str, Any]) -> int:
    target = rule["target"]
    expr = rule["expr"]
    source_columns = "{" + ",".join(rule["sources"]) + "}"
    method = f"rule_v1:{rule['name']}"

    query = f"""
    WITH candidates AS (
        SELECT ticker, report_type, period, ({expr}) AS imputed_value
        FROM financial_reports
        WHERE {target} IS NULL
          AND ({expr}) IS NOT NULL
    ),
    updated AS (
        UPDATE financial_reports fr
        SET {target} = c.imputed_value
        FROM candidates c
        WHERE fr.ticker = c.ticker
          AND fr.report_type = c.report_type
          AND fr.period = c.period
        RETURNING fr.ticker, fr.report_type, fr.period, fr.{target}::text AS imputed_value
    )
    INSERT INTO financial_reports_imputation_log (
        run_id, ticker, report_type, period, column_name,
        imputed_value, method, confidence, source_columns
    )
    SELECT
        %s, ticker, report_type, period, %s,
        imputed_value, %s, 1.0000, %s::text[]
    FROM updated
    """
    cur.execute(query, (run_id, target, method, source_columns))
    return int(cur.rowcount or 0)


def main() -> None:
    load_dotenv()
    db_uri = _get_db_uri()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now().strftime("rule_v1_%Y%m%d_%H%M%S")
    report_path = OUTPUT_DIR / f"imputation_rule_v1_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    rules: list[dict[str, Any]] = [
        {"name": "owner_equity_from_equity", "target": "owner_equity", "expr": "equity", "sources": ["equity"]},
        {"name": "equity_from_owner_equity", "target": "equity", "expr": "owner_equity", "sources": ["owner_equity"]},
        {
            "name": "gross_profit_from_revenue_cogs",
            "target": "gross_profit",
            "expr": "CASE WHEN revenue IS NOT NULL AND cogs IS NOT NULL THEN revenue - cogs END",
            "sources": ["revenue", "cogs"],
        },
        {
            "name": "cogs_from_revenue_gross_profit",
            "target": "cogs",
            "expr": "CASE WHEN revenue IS NOT NULL AND gross_profit IS NOT NULL THEN revenue - gross_profit END",
            "sources": ["revenue", "gross_profit"],
        },
        {
            "name": "ebit_from_operating_profit_interest_expense",
            "target": "ebit",
            "expr": "CASE WHEN operating_profit IS NOT NULL THEN operating_profit + COALESCE(interest_expense, 0) END",
            "sources": ["operating_profit", "interest_expense"],
        },
        {
            "name": "ebitda_from_ebit_depreciation",
            "target": "ebitda",
            "expr": "CASE WHEN ebit IS NOT NULL THEN ebit + COALESCE(depreciation_amortization, 0) END",
            "sources": ["ebit", "depreciation_amortization"],
        },
        {
            "name": "gross_margin_from_gross_profit_revenue",
            "target": "gross_margin",
            "expr": "CASE WHEN revenue IS NOT NULL AND revenue <> 0 AND gross_profit IS NOT NULL THEN gross_profit / revenue END",
            "sources": ["gross_profit", "revenue"],
        },
        {
            "name": "operating_margin_from_operating_profit_revenue",
            "target": "operating_margin",
            "expr": "CASE WHEN revenue IS NOT NULL AND revenue <> 0 AND operating_profit IS NOT NULL THEN operating_profit / revenue END",
            "sources": ["operating_profit", "revenue"],
        },
        {
            "name": "net_margin_from_pat_revenue",
            "target": "net_margin",
            "expr": "CASE WHEN revenue IS NOT NULL AND revenue <> 0 AND profit_after_tax IS NOT NULL THEN profit_after_tax / revenue END",
            "sources": ["profit_after_tax", "revenue"],
        },
        {
            "name": "roe_from_pat_owner_equity",
            "target": "roe",
            "expr": "CASE WHEN COALESCE(owner_equity, equity) IS NOT NULL AND COALESCE(owner_equity, equity) <> 0 AND profit_after_tax IS NOT NULL THEN profit_after_tax / COALESCE(owner_equity, equity) END",
            "sources": ["profit_after_tax", "owner_equity", "equity"],
        },
        {
            "name": "roa_from_pat_assets",
            "target": "roa",
            "expr": "CASE WHEN total_assets IS NOT NULL AND total_assets <> 0 AND profit_after_tax IS NOT NULL THEN profit_after_tax / total_assets END",
            "sources": ["profit_after_tax", "total_assets"],
        },
        {
            "name": "debt_to_equity_from_liabilities_equity",
            "target": "debt_to_equity",
            "expr": "CASE WHEN total_liabilities IS NOT NULL AND COALESCE(owner_equity, equity) IS NOT NULL AND COALESCE(owner_equity, equity) <> 0 THEN total_liabilities / COALESCE(owner_equity, equity) END",
            "sources": ["total_liabilities", "owner_equity", "equity"],
        },
        {
            "name": "current_ratio_from_current_assets_liabilities",
            "target": "current_ratio",
            "expr": "CASE WHEN total_current_assets IS NOT NULL AND total_short_term_liabilities IS NOT NULL AND total_short_term_liabilities <> 0 THEN total_current_assets / total_short_term_liabilities END",
            "sources": ["total_current_assets", "total_short_term_liabilities"],
        },
        {
            "name": "asset_turnover_from_revenue_assets",
            "target": "asset_turnover",
            "expr": "CASE WHEN revenue IS NOT NULL AND total_assets IS NOT NULL AND total_assets <> 0 THEN revenue / total_assets END",
            "sources": ["revenue", "total_assets"],
        },
        {
            "name": "total_equity_and_liabilities_from_assets",
            "target": "total_equity_and_liabilities",
            "expr": "total_assets",
            "sources": ["total_assets"],
        },
    ]

    conn = psycopg2.connect(db_uri)
    conn.autocommit = False
    try:
        summary: dict[str, Any] = {"run_id": run_id, "started_at": datetime.now().isoformat()}
        with conn.cursor() as cur:
            _ensure_imputation_log_table(cur)
            before = _null_profile(cur)

            per_rule_counts: dict[str, int] = {}
            total_imputed = 0
            for rule in rules:
                count = _apply_rule(cur, run_id, rule)
                per_rule_counts[rule["name"]] = count
                total_imputed += count

            conn.commit()

            after = _null_profile(cur)
            summary.update(
                {
                    "success": True,
                    "method": "rule_based_v1",
                    "total_imputed_cells": total_imputed,
                    "rules_applied": per_rule_counts,
                    "before_null_profile": before,
                    "after_null_profile": after,
                    "report_json": str(report_path),
                    "finished_at": datetime.now().isoformat(),
                }
            )

        report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
