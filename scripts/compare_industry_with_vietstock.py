"""
Compare industry in DB (`tickers.industry`) vs Vietstock mapping CSV by ticker.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import get_supabase_client


DEFAULT_VIETSTOCK_CSV = PROJECT_ROOT / "scripts" / "output" / "vietstock_industry_by_stock.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "scripts" / "output"


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_ticker(value: Any) -> str | None:
    text = _norm_text(value)
    if not text:
        return None
    return text.upper()


def _fetch_all_tickers(supabase, page_size: int = 1000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        batch = (
            supabase.table("tickers")
            .select("ticker,industry,exchange,company_name")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
            or []
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def _read_vietstock_map(csv_path: Path) -> dict[str, dict[str, Any]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing file: {csv_path}")
    out: dict[str, dict[str, Any]] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = _norm_ticker(row.get("ticker"))
            if not ticker:
                continue
            out[ticker] = row
    return out


def compare(vietstock_csv: Path, output_dir: Path) -> dict[str, Any]:
    supabase = get_supabase_client()
    db_rows = _fetch_all_tickers(supabase)
    vs_map = _read_vietstock_map(vietstock_csv)

    output_dir.mkdir(parents=True, exist_ok=True)
    all_path = output_dir / "industry_compare_all.csv"
    mismatch_path = output_dir / "industry_compare_mismatch.csv"

    all_fieldnames = [
        "ticker",
        "company_name",
        "exchange",
        "industry_db",
        "industry_vietstock",
        "industry_level_1",
        "industry_level_2",
        "industry_level_3",
        "industry_level_4",
        "industry_path",
        "status",
    ]

    all_rows: list[dict[str, Any]] = []
    mismatch_rows: list[dict[str, Any]] = []
    missing_in_vietstock = 0
    missing_in_db = 0
    matched = 0
    mismatched = 0

    for row in db_rows:
        ticker_raw = row.get("ticker")
        ticker = _norm_ticker(ticker_raw)
        if not ticker:
            continue
        db_ticker = str(ticker_raw)
        db_industry = _norm_text(row.get("industry"))
        vs_row = vs_map.get(ticker)
        vs_industry = _norm_text(vs_row.get("industry")) if vs_row else None

        status: str
        if (not vs_row) or (not vs_industry):
            status = "missing_in_vietstock"
            missing_in_vietstock += 1
        elif not db_industry and vs_industry:
            status = "missing_in_db"
            missing_in_db += 1
        elif db_industry == vs_industry:
            status = "match"
            matched += 1
        else:
            status = "mismatch"
            mismatched += 1

        rec = {
            "ticker": db_ticker,
            "company_name": row.get("company_name"),
            "exchange": row.get("exchange"),
            "industry_db": db_industry,
            "industry_vietstock": vs_industry,
            "industry_level_1": vs_row.get("industry_level_1") if vs_row else None,
            "industry_level_2": vs_row.get("industry_level_2") if vs_row else None,
            "industry_level_3": vs_row.get("industry_level_3") if vs_row else None,
            "industry_level_4": vs_row.get("industry_level_4") if vs_row else None,
            "industry_path": vs_row.get("industry_path") if vs_row else None,
            "status": status,
        }
        all_rows.append(rec)
        if status != "match":
            mismatch_rows.append(rec)

    with all_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    with mismatch_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_fieldnames)
        writer.writeheader()
        writer.writerows(mismatch_rows)

    summary = {
        "success": True,
        "db_rows": len(db_rows),
        "vietstock_rows": len(vs_map),
        "matched": matched,
        "mismatched": mismatched,
        "missing_in_db": missing_in_db,
        "missing_in_vietstock": missing_in_vietstock,
        "all_output_csv": str(all_path),
        "mismatch_output_csv": str(mismatch_path),
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare DB industry with Vietstock industry.")
    parser.add_argument("--vietstock-csv", default=str(DEFAULT_VIETSTOCK_CSV))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    result = compare(Path(args.vietstock_csv), Path(args.output_dir))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
