import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import get_supabase_client

TRUSTED_SOURCES = {"cafef_requests", "vietstock_financeinfo", "cafef_cloudscraper"}


def _iter_rows() -> list[dict[str, Any]]:
    supabase = get_supabase_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("financial_reports")
            .select("ticker,report_type,period,source")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def _is_untrusted_only(source: str | None) -> bool:
    parts = {s.strip() for s in str(source or "").split("+") if s.strip()}
    if not parts:
        return False
    return parts.isdisjoint(TRUSTED_SOURCES)


def main() -> None:
    supabase = get_supabase_client()
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    rows = _iter_rows()
    to_delete = [r for r in rows if _is_untrusted_only(r.get("source"))]
    deleted = 0
    for r in to_delete:
        supabase.table("financial_reports").delete().eq("ticker", r.get("ticker")).eq(
            "report_type", r.get("report_type")
        ).eq("period", r.get("period")).execute()
        deleted += 1

    report = {
        "run_id": run_id,
        "rows_scanned": len(rows),
        "rows_untrusted_only_deleted": deleted,
        "trusted_sources": sorted(list(TRUSTED_SOURCES)),
        "sample_deleted": to_delete[:100],
    }
    report_path = out_dir / f"clean_untrusted_only_rows_{run_id}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report_path)


if __name__ == "__main__":
    main()
