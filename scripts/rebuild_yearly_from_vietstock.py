import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backfill_financial_reports_from_multi_source import (  # noqa: E402
    SOURCE_CONFIDENCE,
    fetch_from_vietstock_financeinfo,
)
from scripts.config import get_supabase_client  # noqa: E402


def _year_from_period(period: str) -> int | None:
    m = re.search(r"([0-9]{4})", str(period or ""))
    return int(m.group(1)) if m else None


def _fetch_tickers(supabase, limit: int = 0) -> list[str]:
    out: list[str] = []
    offset = 0
    page_size = 1000
    while True:
        rows = (
            supabase.table("tickers")
            .select("ticker")
            .order("ticker")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
            or []
        )
        if not rows:
            break
        out.extend(str(r.get("ticker")) for r in rows if r.get("ticker"))
        if len(rows) < page_size:
            break
        offset += page_size
    if limit > 0:
        return out[:limit]
    return out


def _fetch_existing_year_period_aliases(supabase, tickers: list[str]) -> dict[tuple[str, int], str]:
    aliases: dict[tuple[str, int], str] = {}
    if not tickers:
        return aliases

    offset = 0
    page_size = 1000
    while True:
        rows = (
            supabase.table("financial_reports")
            .select("ticker,report_type,period")
            .in_("ticker", tickers)
            .eq("report_type", "yearly")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
            or []
        )
        if not rows:
            break
        for r in rows:
            ticker = str(r.get("ticker") or "")
            period = str(r.get("period") or "")
            year = _year_from_period(period)
            if not ticker or year is None:
                continue
            key = (ticker, year)
            # Prefer existing "FY-YYYY" alias to avoid duplicate yearly keys.
            if key not in aliases or period.upper().startswith("FY-"):
                aliases[key] = period
        if len(rows) < page_size:
            break
        offset += page_size
    return aliases


def _batch_upsert(supabase, rows: list[dict[str, Any]], batch_size: int = 200) -> int:
    updated = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        supabase.table("financial_reports").upsert(batch, on_conflict="ticker,report_type,period").execute()
        updated += len(batch)
    return updated


def run_rebuild(
    tickers: list[str],
    workers: int = 8,
    sleep_ms: int = 0,
) -> dict[str, Any]:
    supabase = get_supabase_client()
    aliases = _fetch_existing_year_period_aliases(supabase, tickers)

    rows_to_upsert: list[dict[str, Any]] = []
    stats = {
        "tickers_total": len(tickers),
        "tickers_ok": 0,
        "tickers_failed": 0,
        "records_from_vietstock": 0,
        "records_to_upsert": 0,
        "records_upserted": 0,
    }
    failed: list[dict[str, str]] = []

    def _process_ticker(ticker: str) -> list[dict[str, Any]]:
        recs = fetch_from_vietstock_financeinfo(ticker)
        out: list[dict[str, Any]] = []
        for rec in recs:
            year = _year_from_period(str(rec.get("period") or ""))
            if year is None:
                continue
            period = aliases.get((ticker, year), f"FY-{year}")
            rec["period"] = period
            rec["source"] = "vietstock_financeinfo"
            rec["confidence"] = SOURCE_CONFIDENCE.get("vietstock_financeinfo", 0.88)
            out.append(rec)
        return out

    with ThreadPoolExecutor(max_workers=max(workers, 1)) as ex:
        futures = {ex.submit(_process_ticker, t): t for t in tickers}
        for fut in as_completed(futures):
            ticker = futures[fut]
            try:
                out = fut.result()
                stats["tickers_ok"] += 1
                stats["records_from_vietstock"] += len(out)
                rows_to_upsert.extend(out)
            except Exception as exc:  # noqa: BLE001
                stats["tickers_failed"] += 1
                failed.append({"ticker": ticker, "error": str(exc)})
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)

    stats["records_to_upsert"] = len(rows_to_upsert)
    stats["records_upserted"] = _batch_upsert(supabase, rows_to_upsert)

    # Refresh gold table if function exists.
    refresh_ok = False
    refresh_error = None
    try:
        supabase.rpc("refresh_financial_reports_bi_gold").execute()
        refresh_ok = True
    except Exception as exc:  # noqa: BLE001
        refresh_error = str(exc)

    return {
        "generated_at": datetime.now().isoformat(),
        "stats": stats,
        "refresh_bi_gold_ok": refresh_ok,
        "refresh_bi_gold_error": refresh_error,
        "failed_sample": failed[:30],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild yearly financial_reports from vietstock_financeinfo.")
    parser.add_argument("--tickers", type=str, default="", help="Comma-separated ticker list. Empty = all tickers.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of tickers if --tickers is empty.")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers.")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Sleep between completed futures.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    if args.tickers.strip():
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = _fetch_tickers(supabase, limit=max(args.limit, 0))

    report = run_rebuild(tickers=tickers, workers=max(args.workers, 1), sleep_ms=max(args.sleep_ms, 0))

    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = out_dir / f"rebuild_yearly_from_vietstock_{run_id}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report_path)
    print(json.dumps(report["stats"], ensure_ascii=False))


if __name__ == "__main__":
    main()
