from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql


TABLES = ["tickers", "daily_prices", "financial_reports"]


def _get_db_uri() -> str:
    uri = os.getenv("SUPABASE_DB_URI") or os.getenv("DATABASE_URL")
    if not uri:
        raise RuntimeError("Missing SUPABASE_DB_URI or DATABASE_URL")
    return uri


def _get_columns(cur, table_name: str) -> list[str]:
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
        order by ordinal_position
        """,
        (table_name,),
    )
    return [r[0] for r in cur.fetchall()]


def _status(null_rate_pct: float) -> str:
    if null_rate_pct >= 60:
        return "critical"
    if null_rate_pct >= 30:
        return "high"
    if null_rate_pct >= 10:
        return "medium"
    return "healthy"


def profile_table(cur, table_name: str) -> list[dict]:
    columns = _get_columns(cur, table_name)
    cur.execute(sql.SQL("select count(*) from {}").format(sql.Identifier(table_name)))
    total_rows = int(cur.fetchone()[0] or 0)

    rows = []
    for col in columns:
        query = sql.SQL("select count(*) from {} where {} is null").format(
            sql.Identifier(table_name),
            sql.Identifier(col),
        )
        cur.execute(query)
        null_count = int(cur.fetchone()[0] or 0)
        null_rate = (null_count * 100.0 / total_rows) if total_rows else 0.0
        rows.append(
            {
                "table_name": table_name,
                "column_name": col,
                "null_count": null_count,
                "total_rows": total_rows,
                "null_rate_pct": round(null_rate, 2),
                "fill_rate_pct": round(100.0 - null_rate, 2),
                "status": _status(null_rate),
            }
        )
    rows.sort(key=lambda r: r["null_rate_pct"], reverse=True)
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "table_name",
                "column_name",
                "null_count",
                "total_rows",
                "null_rate_pct",
                "fill_rate_pct",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    load_dotenv()
    db_uri = _get_db_uri()
    out_dir = Path("scripts/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {}
    with psycopg2.connect(db_uri) as conn:
        with conn.cursor() as cur:
            for table in TABLES:
                rows = profile_table(cur, table)
                out_csv = out_dir / f"null_profile_all_columns_{table}.csv"
                write_csv(out_csv, rows)
                summary[table] = {
                    "total_columns": len(rows),
                    "top5": rows[:5],
                    "output_csv": str(out_csv),
                }
                print(f"[OK] {table}: columns={len(rows)} -> {out_csv}")

    out_json = out_dir / "null_profile_all_columns_summary.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[SUMMARY] {out_json}")


if __name__ == "__main__":
    main()
