"""
Profile NULL density for key warehouse tables, with focus on financial_reports.

Outputs:
- Console summary sorted by highest NULL rate
- CSV report under scripts/output/null_profile_<table>.csv
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


DEFAULT_TABLE = "financial_reports"
ALLOWED_TABLES = {"financial_reports", "daily_prices", "tickers"}


def _get_db_uri() -> str:
    uri = os.getenv("SUPABASE_DB_URI") or os.getenv("DATABASE_URL")
    if not uri:
        raise RuntimeError("Missing SUPABASE_DB_URI or DATABASE_URL")
    return uri


def _get_numeric_columns(cur, table_name: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND data_type IN ('smallint', 'integer', 'bigint', 'numeric', 'real', 'double precision')
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    return [r[0] for r in cur.fetchall()]


def _get_base_count(cur, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cur.fetchone()[0])


def _build_null_sql(table_name: str, columns: list[str]) -> str:
    agg_parts = [f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_null" for c in columns]
    return f"SELECT COUNT(*) AS total_rows, {', '.join(agg_parts)} FROM {table_name}"


def _write_csv(rows: list[dict], table_name: str) -> Path:
    output_dir = Path("scripts/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"null_profile_{table_name}.csv"
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["column_name", "null_count", "total_rows", "null_rate_pct", "fill_rate_pct", "status"],
        )
        writer.writeheader()
        writer.writerows(rows)
    return out_path


def _status_from_null_rate(null_rate_pct: float) -> str:
    if null_rate_pct >= 60:
        return "critical"
    if null_rate_pct >= 30:
        return "high"
    if null_rate_pct >= 10:
        return "medium"
    return "healthy"


def profile_table(table_name: str):
    load_dotenv()
    db_uri = _get_db_uri()
    with psycopg2.connect(db_uri) as conn:
        with conn.cursor() as cur:
            columns = _get_numeric_columns(cur, table_name)
            if not columns:
                raise RuntimeError(f"No numeric columns found for table {table_name}")

            total_rows = _get_base_count(cur, table_name)
            cur.execute(_build_null_sql(table_name, columns))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to fetch NULL profile")

            prof_rows = []
            idx = 1
            for col in columns:
                null_count = int(row[idx] or 0)
                idx += 1
                null_rate = (null_count * 100.0 / total_rows) if total_rows else 0.0
                fill_rate = 100.0 - null_rate
                prof_rows.append(
                    {
                        "column_name": col,
                        "null_count": null_count,
                        "total_rows": total_rows,
                        "null_rate_pct": round(null_rate, 2),
                        "fill_rate_pct": round(fill_rate, 2),
                        "status": _status_from_null_rate(null_rate),
                    }
                )

            prof_rows.sort(key=lambda r: r["null_rate_pct"], reverse=True)
            out_path = _write_csv(prof_rows, table_name)

            print(f"[NULL PROFILE] table={table_name}, rows={total_rows}, numeric_cols={len(columns)}")
            print("[TOP NULL COLUMNS]")
            for r in prof_rows[:15]:
                print(
                    f"  - {r['column_name']}: null={r['null_count']}/{r['total_rows']} "
                    f"({r['null_rate_pct']:.2f}%), status={r['status']}"
                )
            print(f"[CSV] {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate NULL profile for warehouse table.")
    parser.add_argument("--table", default=DEFAULT_TABLE, help=f"Table to profile: {sorted(ALLOWED_TABLES)}")
    args = parser.parse_args()

    table_name = (args.table or "").strip()
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported table '{table_name}'. Allowed: {sorted(ALLOWED_TABLES)}")

    profile_table(table_name)


if __name__ == "__main__":
    main()
