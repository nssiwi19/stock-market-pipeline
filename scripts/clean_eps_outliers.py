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
EPS_TRUSTED_SOURCES = {"cafef_requests", "vietstock_financeinfo", "cafef_cloudscraper"}
API_RETRY_ATTEMPTS = 5


def _execute_with_retry(action):
    last_exc: Exception | None = None
    for i in range(1, API_RETRY_ATTEMPTS + 1):
        try:
            return action()
        except Exception as exc:
            last_exc = exc
            if i >= API_RETRY_ATTEMPTS:
                raise
            time.sleep(1.5 * i)
    if last_exc:
        raise last_exc
    raise RuntimeError("Unexpected retry state")


def _iter_financial_rows() -> list[dict[str, Any]]:
    supabase = get_supabase_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 1000
    while True:
        resp = (
            _execute_with_retry(
                lambda: supabase.table("financial_reports")
                .select("ticker,report_type,period,eps,source")
                .range(offset, offset + page_size - 1)
                .execute()
            )
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def _should_null_eps(row: dict[str, Any]) -> str | None:
    eps = row.get("eps")
    if eps is None:
        return None
    try:
        val = float(eps)
    except (TypeError, ValueError):
        return "non_numeric"
    if not math.isfinite(val):
        return "non_finite"
    if abs(val) > EPS_MAX_ABS:
        return "outlier_abs"
    src = str(row.get("source") or "")
    src_parts = {s.strip() for s in src.split("+") if s.strip()}
    if src_parts and src_parts.isdisjoint(EPS_TRUSTED_SOURCES):
        return "untrusted_source"
    return None


def main() -> None:
    supabase = get_supabase_client()
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    rows = _iter_financial_rows()
    updates: list[dict[str, Any]] = []
    reason_count = {
        "outlier_abs": 0,
        "untrusted_source": 0,
        "non_numeric": 0,
        "non_finite": 0,
    }
    sample: list[dict[str, Any]] = []

    for row in rows:
        reason = _should_null_eps(row)
        if reason is None:
            continue
        reason_count[reason] = reason_count.get(reason, 0) + 1
        updates.append(
            {
                "ticker": row.get("ticker"),
                "report_type": row.get("report_type"),
                "period": row.get("period"),
                "eps": None,
            }
        )
        if len(sample) < 100:
            sample.append(
                {
                    "ticker": row.get("ticker"),
                    "report_type": row.get("report_type"),
                    "period": row.get("period"),
                    "eps_before": row.get("eps"),
                    "source": row.get("source"),
                    "reason": reason,
                }
            )

    # De-duplicate updates by key.
    unique_updates: dict[tuple[str, str, str], dict[str, Any]] = {}
    for u in updates:
        key = (str(u.get("ticker") or ""), str(u.get("report_type") or ""), str(u.get("period") or ""))
        unique_updates[key] = u
    update_rows = list(unique_updates.values())

    batch_size = 200
    updated = 0
    for i in range(0, len(update_rows), batch_size):
        batch = update_rows[i : i + batch_size]
        _execute_with_retry(
            lambda b=batch: supabase.table("financial_reports")
            .upsert(b, on_conflict="ticker,report_type,period")
            .execute()
        )
        updated += len(batch)

    report = {
        "run_id": run_id,
        "total_rows_scanned": len(rows),
        "eps_rows_cleaned": updated,
        "reason_count": reason_count,
        "eps_max_abs": EPS_MAX_ABS,
        "trusted_sources": sorted(list(EPS_TRUSTED_SOURCES)),
        "sample": sample,
    }

    report_path = out_dir / f"eps_cleanup_report_{run_id}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report_path)


if __name__ == "__main__":
    main()
