"""
Backfill derived metrics in financial_reports from existing core columns.
This script does NOT fabricate unavailable raw metrics; it only computes
deterministic fields from available values.
"""

from __future__ import annotations

import os
from typing import Dict, Tuple

import psycopg2
from dotenv import load_dotenv


def _get_db_uri() -> str:
    uri = os.getenv("SUPABASE_DB_URI") or os.getenv("DATABASE_URL")
    if not uri:
        raise RuntimeError("Missing SUPABASE_DB_URI or DATABASE_URL")
    return uri


def _fetch_null_stats(cur) -> Dict[str, Tuple[int, int]]:
    cols = [
        "revenue",
        "profit_after_tax",
        "total_assets",
        "total_liabilities",
        "owner_equity",
        "cogs",
        "gross_profit",
        "cash_flow_operating",
        "ebit",
        "ebitda",
        "gross_margin",
        "net_margin",
        "roe",
        "roa",
        "debt_to_equity",
        "current_ratio",
        "asset_turnover",
    ]
    sql = """
    SELECT
      COUNT(*) AS total_rows,
      {agg_sql}
    FROM financial_reports
    """
    agg_parts = []
    for c in cols:
        agg_parts.append(f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_null")
    cur.execute(sql.format(agg_sql=",\n      ".join(agg_parts)))
    row = cur.fetchone()
    total = row[0]
    stats = {}
    idx = 1
    for c in cols:
        stats[c] = (row[idx], total)
        idx += 1
    return stats


def _print_stats(title: str, stats: Dict[str, Tuple[int, int]]):
    print(title)
    for col, (nulls, total) in stats.items():
        pct = (nulls * 100.0 / total) if total else 0.0
        print(f"  - {col}: null={nulls}/{total} ({pct:.2f}%)")


def main():
    load_dotenv()
    db_uri = _get_db_uri()
    conn = psycopg2.connect(db_uri)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            before = _fetch_null_stats(cur)
            _print_stats("[BEFORE] Null profile", before)

            # Deterministic backfills only
            cur.execute(
                """
                UPDATE financial_reports
                SET
                    owner_equity = COALESCE(owner_equity, equity),
                    equity = COALESCE(equity, owner_equity),
                    ebit = COALESCE(ebit, COALESCE(operating_profit, 0) + COALESCE(interest_expense, 0)),
                    ebitda = COALESCE(ebitda, COALESCE(ebit, COALESCE(operating_profit, 0) + COALESCE(interest_expense, 0)) + COALESCE(depreciation_amortization, 0)),
                    cogs = COALESCE(cogs, CASE WHEN revenue IS NOT NULL AND gross_profit IS NOT NULL THEN revenue - gross_profit END),
                    gross_profit = COALESCE(gross_profit, CASE WHEN revenue IS NOT NULL AND cogs IS NOT NULL THEN revenue - cogs END),
                    gross_margin = COALESCE(gross_margin, CASE WHEN revenue IS NOT NULL AND revenue <> 0 AND gross_profit IS NOT NULL THEN gross_profit / revenue END),
                    net_margin = COALESCE(net_margin, CASE WHEN revenue IS NOT NULL AND revenue <> 0 AND profit_after_tax IS NOT NULL THEN profit_after_tax / revenue END),
                    roe = COALESCE(roe, CASE WHEN owner_equity IS NOT NULL AND owner_equity <> 0 AND profit_after_tax IS NOT NULL THEN profit_after_tax / owner_equity END),
                    roa = COALESCE(roa, CASE WHEN total_assets IS NOT NULL AND total_assets <> 0 AND profit_after_tax IS NOT NULL THEN profit_after_tax / total_assets END),
                    debt_to_equity = COALESCE(debt_to_equity, CASE WHEN owner_equity IS NOT NULL AND owner_equity <> 0 AND total_liabilities IS NOT NULL THEN total_liabilities / owner_equity END),
                    current_ratio = COALESCE(current_ratio, CASE WHEN total_short_term_liabilities IS NOT NULL AND total_short_term_liabilities <> 0 AND total_current_assets IS NOT NULL THEN total_current_assets / total_short_term_liabilities END),
                    asset_turnover = COALESCE(asset_turnover, CASE WHEN total_assets IS NOT NULL AND total_assets <> 0 AND revenue IS NOT NULL THEN revenue / total_assets END),
                    total_equity_and_liabilities = COALESCE(total_equity_and_liabilities, total_assets)
                """
            )
            updated = cur.rowcount
            print(f"[UPDATE] Rows touched: {updated}")
            conn.commit()

            after = _fetch_null_stats(cur)
            _print_stats("[AFTER] Null profile", after)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
