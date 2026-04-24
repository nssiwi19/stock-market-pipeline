import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from vnstock import Company

if "__file__" in globals():
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
else:
    PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import get_supabase_client


OUTPUT_DIR = PROJECT_ROOT / "scripts" / "output"

PHONE_KEYS = (
    "phone",
    "telephone",
    "tel",
    "hotline",
    "company_phone",
    "companyPhone",
    "contact_phone",
    "contactPhone",
)


def _normalize_phone(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan", "n/a", "unknown"}:
        return None
    normalized = re.sub(r"[^0-9+\-\s().]", "", text).strip()
    digits = len(re.sub(r"\D", "", normalized))
    if digits < 8:
        return None
    return normalized


def _extract_phone_from_overview_row(row: dict) -> str | None:
    for key in PHONE_KEYS:
        if key in row:
            phone = _normalize_phone(row.get(key))
            if phone:
                return phone
    return None


def _fetch_phone_from_overview(ticker: str, max_attempts: int = 2) -> tuple[str | None, str | None]:
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            df = Company(symbol=ticker).overview()
            if df is None or getattr(df, "empty", True):
                return None, None
            row = df.iloc[0].to_dict()
            return _extract_phone_from_overview_row(row), None
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            if attempt < max_attempts:
                wait_s = 3
                m = re.search(r"(\d+)\s*gi[âa]y", last_error, flags=re.IGNORECASE)
                if m:
                    try:
                        wait_s = max(int(m.group(1)), 3)
                    except ValueError:
                        wait_s = 3
                time.sleep(wait_s)
    return None, last_error


def _fetch_missing_phone_tickers(limit: int = 0) -> list[str]:
    supabase = get_supabase_client()
    out: list[str] = []
    offset = 0
    page_size = 1000
    while True:
        query = (
            supabase.table("tickers")
            .select("ticker")
            .is_("contact_phone", "null")
            .order("ticker")
            .range(offset, offset + page_size - 1)
        )
        rows = query.execute().data or []
        if not rows:
            break
        out.extend(str(r.get("ticker")) for r in rows if r.get("ticker"))
        if len(rows) < page_size:
            break
        offset += page_size
    if limit > 0:
        return out[:limit]
    return out


def run_backfill(limit: int, max_rpm: int) -> dict:
    supabase = get_supabase_client()
    tickers = _fetch_missing_phone_tickers(limit=limit)
    delay_s = max(60.0 / max(max_rpm, 1), 0.0)

    updates: list[dict] = []
    failed: list[dict] = []

    for idx, ticker in enumerate(tickers, start=1):
        phone, error = _fetch_phone_from_overview(ticker)
        if phone:
            updates.append({"ticker": ticker, "contact_phone": phone})
        elif error:
            failed.append({"ticker": ticker, "error": error})
        if idx < len(tickers) and delay_s > 0:
            time.sleep(delay_s)

    updated = 0
    batch_size = 100
    for i in range(0, len(updates), batch_size):
        batch = updates[i : i + batch_size]
        supabase.table("tickers").upsert(batch, on_conflict="ticker").execute()
        updated += len(batch)

    total = supabase.table("tickers").select("ticker", count="exact", head=True).execute().count or 0
    filled = (
        supabase.table("tickers")
        .select("ticker", count="exact", head=True)
        .not_.is_("contact_phone", "null")
        .execute()
        .count
        or 0
    )

    return {
        "generated_at": datetime.now().isoformat(),
        "tickers_scanned": len(tickers),
        "tickers_with_phone_found": len(updates),
        "tickers_updated": updated,
        "tickers_failed": len(failed),
        "coverage": {
            "total_tickers": total,
            "filled_contact_phone": filled,
        },
        "failed_sample": failed[:30],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill contact phone for tickers from vnstock Company.overview.")
    parser.add_argument("--limit", type=int, default=0, help="Max tickers to scan. 0 = all missing.")
    parser.add_argument("--max-rpm", type=int, default=20, help="Max requests per minute to avoid rate limit.")
    parser.add_argument(
        "--output-file",
        type=str,
        default="",
        help="Optional fixed output file path for deterministic reporting.",
    )
    args = parser.parse_args()

    report = run_backfill(limit=max(args.limit, 0), max_rpm=max(args.max_rpm, 1))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = Path(args.output_file) if str(args.output_file).strip() else (OUTPUT_DIR / f"backfill_ticker_contact_phone_{run_id}.json")
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(path)
    print(json.dumps(report["coverage"], ensure_ascii=False))


if __name__ == "__main__":
    main()
