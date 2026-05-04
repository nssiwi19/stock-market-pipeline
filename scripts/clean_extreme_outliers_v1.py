import csv
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import get_supabase_client


EPS_MAX_ABS = 100_000.0
RATIO_MAX_ABS = 100.0
API_RETRY_ATTEMPTS = 5

RATIO_COLUMNS = [
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roa",
    "debt_to_equity",
    "current_ratio",
    "asset_turnover",
]
TARGET_COLUMNS = ["eps"] + RATIO_COLUMNS


def _execute_with_retry(action):
    last_exc: Exception | None = None
    for i in range(1, API_RETRY_ATTEMPTS + 1):
        try:
            return action()
        except Exception as exc:
            last_exc = exc
            if i >= API_RETRY_ATTEMPTS:
                raise
            time.sleep(1.2 * i)
    if last_exc:
        raise last_exc
    raise RuntimeError("Unexpected retry state")


def _iter_rows() -> list[dict[str, Any]]:
    supabase = get_supabase_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 1000
    select_cols = ",".join(["ticker", "report_type", "period", "source"] + TARGET_COLUMNS)
    while True:
        resp = _execute_with_retry(
            lambda: supabase.table("financial_reports")
            .select(select_cols)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def _to_num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _threshold(col: str) -> float:
    return EPS_MAX_ABS if col == "eps" else RATIO_MAX_ABS


def main() -> None:
    supabase = get_supabase_client()
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    rows = _iter_rows()
    updates: list[dict[str, Any]] = []
    cleaned_by_column = {c: 0 for c in TARGET_COLUMNS}
    reason_count = {"non_numeric_or_non_finite": 0, "outlier_abs": 0}
    sample: list[dict[str, Any]] = []

    for row in rows:
        changed = False
        update_row: dict[str, Any] = {
            "ticker": row.get("ticker"),
            "report_type": row.get("report_type"),
            "period": row.get("period"),
        }
        for col in TARGET_COLUMNS:
            raw = row.get(col)
            if raw is None:
                continue
            num = _to_num(raw)
            if num is None:
                update_row[col] = None
                changed = True
                cleaned_by_column[col] += 1
                reason_count["non_numeric_or_non_finite"] += 1
                if len(sample) < 200:
                    sample.append(
                        {
                            "ticker": row.get("ticker"),
                            "report_type": row.get("report_type"),
                            "period": row.get("period"),
                            "source": row.get("source"),
                            "column_name": col,
                            "value_before": raw,
                            "reason": "non_numeric_or_non_finite",
                        }
                    )
                continue
            if abs(num) > _threshold(col):
                update_row[col] = None
                changed = True
                cleaned_by_column[col] += 1
                reason_count["outlier_abs"] += 1
                if len(sample) < 200:
                    sample.append(
                        {
                            "ticker": row.get("ticker"),
                            "report_type": row.get("report_type"),
                            "period": row.get("period"),
                            "source": row.get("source"),
                            "column_name": col,
                            "value_before": num,
                            "reason": "outlier_abs",
                            "threshold_abs": _threshold(col),
                        }
                    )
        if changed:
            updates.append(update_row)

    # de-duplicate by key
    dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for u in updates:
        key = (str(u.get("ticker") or ""), str(u.get("report_type") or ""), str(u.get("period") or ""))
        if key not in dedup:
            dedup[key] = {"ticker": u["ticker"], "report_type": u["report_type"], "period": u["period"]}
        for col in TARGET_COLUMNS:
            if col in u:
                dedup[key][col] = u[col]
    update_rows = list(dedup.values())

    batch_size = 200
    rows_updated = 0
    for i in range(0, len(update_rows), batch_size):
        batch = update_rows[i : i + batch_size]
        _execute_with_retry(
            lambda b=batch: supabase.table("financial_reports")
            .upsert(b, on_conflict="ticker,report_type,period")
            .execute()
        )
        rows_updated += len(batch)

    cells_cleaned = int(sum(cleaned_by_column.values()))
    report = {
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(),
        "rows_scanned": len(rows),
        "rows_updated": rows_updated,
        "cells_cleaned": cells_cleaned,
        "thresholds": {"eps_max_abs": EPS_MAX_ABS, "ratio_max_abs": RATIO_MAX_ABS},
        "cleaned_by_column": cleaned_by_column,
        "reason_count": reason_count,
        "sample": sample,
    }

    report_json = out_dir / f"clean_extreme_outliers_v1_{run_id}.json"
    report_csv = out_dir / f"clean_extreme_outliers_v1_sample_{run_id}.csv"
    report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    with report_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ticker",
                "report_type",
                "period",
                "source",
                "column_name",
                "value_before",
                "reason",
                "threshold_abs",
            ],
        )
        writer.writeheader()
        writer.writerows(sample)

    print(report_json)
    print(report_csv)


if __name__ == "__main__":
    main()
