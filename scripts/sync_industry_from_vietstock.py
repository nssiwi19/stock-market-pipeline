"""
Sync ticker -> industry mapping from Vietstock GICS endpoints.

Flow:
1) Get anti-forgery token from https://finance.vietstock.vn/chi-so-nganh.htm
2) Fetch full industry list from /Data/GetListIndustryGICS
3) For each level-1 industry code, fetch /Data/GetHeatmapGICS with detailStockCode=true
4) Build ticker mapping from level-5 nodes (stock leaf)
5) Export audit CSV/JSON and optionally upsert to Supabase `tickers.industry`
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import get_supabase_client


SOURCE_URL = "https://finance.vietstock.vn/chi-so-nganh.htm"
LIST_URL = "https://finance.vietstock.vn/Data/GetListIndustryGICS"
HEATMAP_URL = "https://finance.vietstock.vn/Data/GetHeatmapGICS"
OUTPUT_DIR = PROJECT_ROOT / "scripts" / "output"

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

XHR_HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://finance.vietstock.vn",
    "Referer": SOURCE_URL,
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}


def _extract_token(html: str) -> str:
    match = re.search(
        r'name=["\']?__RequestVerificationToken["\']?[^>]*value=["\']?([^"\'>\s]+)',
        html,
        flags=re.IGNORECASE,
    )
    if not match:
        raise RuntimeError("Could not find __RequestVerificationToken from source page.")
    return match.group(1)


def _fetch_token(session: requests.Session) -> str:
    response = session.get(SOURCE_URL, headers=BASE_HEADERS, timeout=30)
    response.raise_for_status()
    return _extract_token(response.text)


def _fetch_industry_list(session: requests.Session, token: str) -> list[dict[str, Any]]:
    headers = dict(BASE_HEADERS)
    headers.update(XHR_HEADERS)
    response = session.post(
        LIST_URL,
        data={"__RequestVerificationToken": token},
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError("GetListIndustryGICS did not return a list.")
    return payload


def _fetch_heatmap(session: requests.Session, token: str, industry_code: int) -> list[dict[str, Any]]:
    headers = dict(BASE_HEADERS)
    headers.update(XHR_HEADERS)
    response = session.post(
        HEATMAP_URL,
        data={
            "industryCode": str(industry_code),
            "detailIndustry": "true",
            "detailStockCode": "true",
            "__RequestVerificationToken": token,
        },
        headers=headers,
        timeout=90,
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError(
            f"GetHeatmapGICS returned non-JSON for industryCode={industry_code}"
        ) from exc
    if not isinstance(payload, list):
        raise RuntimeError(f"Heatmap payload must be a list, got: {type(payload)}")
    return payload


def _safe_int(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_ticker(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if len(text) > 6:
        return None
    if not re.fullmatch(r"[A-Z0-9\.]+", text):
        return None
    return text


def _build_parent_lookup(
    all_rows: list[dict[str, Any]],
) -> tuple[dict[int, dict[str, Any]], dict[int, list[dict[str, Any]]]]:
    by_code: dict[int, dict[str, Any]] = {}
    children: dict[int, list[dict[str, Any]]] = {}
    for row in all_rows:
        code = _safe_int(row.get("IndustryCode"))
        parent = _safe_int(row.get("ParentCode"))
        if code < 0:
            continue
        by_code[code] = row
        children.setdefault(parent, []).append(row)
    return by_code, children


def _build_path_names(row: dict[str, Any], by_code: dict[int, dict[str, Any]]) -> list[str]:
    names: list[str] = []
    current = row
    seen: set[int] = set()
    while current:
        code = _safe_int(current.get("IndustryCode"))
        parent_code = _safe_int(current.get("ParentCode"))
        name = str(current.get("IndustryName") or "").strip()
        if name:
            names.append(name)
        if code in seen or parent_code <= 0:
            break
        seen.add(code)
        current = by_code.get(parent_code)
    names.reverse()
    return names


def _build_stock_mapping(all_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Counter]:
    by_code, _ = _build_parent_lookup(all_rows)
    level_counter = Counter(_safe_int(row.get("Level")) for row in all_rows)

    mapped: list[dict[str, Any]] = []
    for row in all_rows:
        if _safe_int(row.get("Level")) != 5:
            continue

        ticker = _normalize_ticker(row.get("IndustryName"))
        if not ticker:
            continue

        stock_name = str(row.get("StockFullName") or "").strip() or None
        path_names = _build_path_names(row, by_code)
        if path_names and path_names[-1] == ticker:
            path_names = path_names[:-1]

        level_1 = path_names[0] if len(path_names) > 0 else None
        level_2 = path_names[1] if len(path_names) > 1 else None
        level_3 = path_names[2] if len(path_names) > 2 else None
        level_4 = path_names[3] if len(path_names) > 3 else None
        industry = level_4 or level_3 or level_2 or level_1

        mapped.append(
            {
                "ticker": ticker,
                "stock_full_name": stock_name,
                "industry": industry,
                "industry_level_1": level_1,
                "industry_level_2": level_2,
                "industry_level_3": level_3,
                "industry_level_4": level_4,
                "industry_path": " > ".join(path_names) if path_names else None,
                "vietstock_leaf_code": _safe_int(row.get("IndustryCode")),
                "vietstock_parent_code": _safe_int(row.get("ParentCode")),
            }
        )

    dedup: dict[str, dict[str, Any]] = {}
    for item in mapped:
        dedup[item["ticker"]] = item
    return sorted(dedup.values(), key=lambda x: x["ticker"]), level_counter


def _write_outputs(
    mapping_rows: list[dict[str, Any]],
    level_counter: Counter,
    total_heatmap_rows: int,
    top_level_codes: list[int],
) -> dict[str, str]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / "vietstock_industry_by_stock.csv"
    json_path = OUTPUT_DIR / "vietstock_industry_sync_summary.json"

    fieldnames = [
        "ticker",
        "stock_full_name",
        "industry",
        "industry_level_1",
        "industry_level_2",
        "industry_level_3",
        "industry_level_4",
        "industry_path",
        "vietstock_leaf_code",
        "vietstock_parent_code",
    ]
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in mapping_rows:
            writer.writerow(row)

    summary = {
        "source_url": SOURCE_URL,
        "top_level_codes": top_level_codes,
        "total_heatmap_rows": total_heatmap_rows,
        "level_distribution": dict(level_counter),
        "tickers_mapped": len(mapping_rows),
        "output_csv": str(csv_path),
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"csv": str(csv_path), "summary": str(json_path)}


def _fetch_all_existing_tickers(supabase, page_size: int = 1000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        batch = (
            supabase.table("tickers")
            .select("ticker,industry")
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


def _update_single_ticker_industry_with_retry(
    ticker: str,
    industry: str,
    max_attempts: int = 3,
) -> bool:
    """Update one ticker with retry to survive transient network/http2 errors."""
    for attempt in range(1, max_attempts + 1):
        try:
            client = get_supabase_client()
            client.table("tickers").update({"industry": industry}).eq("ticker", ticker).execute()
            return True
        except BaseException:
            if attempt == max_attempts:
                return False
            time.sleep(0.6 * attempt)
    return False


def _upsert_industry_to_supabase(mapping_rows: list[dict[str, Any]], batch_size: int = 200) -> dict[str, int]:
    supabase = get_supabase_client()
    db_rows = _fetch_all_existing_tickers(supabase)
    db_ticker_map = {
        str(r.get("ticker")).upper(): str(r.get("ticker"))
        for r in db_rows
        if r.get("ticker")
    }

    updates = []
    for row in mapping_rows:
        ticker_upper = row["ticker"]
        db_ticker = db_ticker_map.get(ticker_upper)
        if not db_ticker:
            continue
        industry = row.get("industry")
        if not industry:
            continue
        updates.append({"ticker": db_ticker, "industry": industry})

    if not updates:
        return {"db_tickers": len(db_ticker_map), "updates_prepared": 0, "updated_rows": 0}

    # Use UPDATE-by-ticker instead of UPSERT to avoid accidental INSERT rows
    # that can violate NOT NULL constraints (e.g., exchange).
    updated = 0
    failed = 0
    failed_tickers: list[str] = []
    for idx, rec in enumerate(updates, start=1):
        ticker = rec["ticker"]
        industry = rec["industry"]
        ok = _update_single_ticker_industry_with_retry(ticker=ticker, industry=industry, max_attempts=3)
        if ok:
            updated += 1
        else:
            failed += 1
            failed_tickers.append(ticker)
        if idx % batch_size == 0:
            print(f"[DB] updated {updated}/{len(updates)} | failed={failed}")
            time.sleep(0.2)

    return {
        "db_tickers": len(db_ticker_map),
        "updates_prepared": len(updates),
        "updated_rows": updated,
        "failed_updates": failed,
        "failed_tickers_preview": failed_tickers[:20],
    }


def run_sync(apply_to_db: bool, sleep_seconds: float) -> dict[str, Any]:
    session = requests.Session()
    token = _fetch_token(session)
    industry_list = _fetch_industry_list(session, token)
    top_level_codes = sorted(
        {
            _safe_int(item.get("IndustryCode"))
            for item in industry_list
            if _safe_int(item.get("Level")) == 1 and _safe_int(item.get("IndustryCode")) > 0
        }
    )

    all_rows: list[dict[str, Any]] = []
    for idx, code in enumerate(top_level_codes, start=1):
        rows = _fetch_heatmap(session, token, code)
        all_rows.extend(rows)
        print(f"[{idx}/{len(top_level_codes)}] fetched industryCode={code}, rows={len(rows)}")
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    mapping_rows, level_counter = _build_stock_mapping(all_rows)
    output_paths = _write_outputs(mapping_rows, level_counter, len(all_rows), top_level_codes)

    result: dict[str, Any] = {
        "success": True,
        "top_level_codes": top_level_codes,
        "total_heatmap_rows": len(all_rows),
        "level_distribution": dict(level_counter),
        "tickers_mapped": len(mapping_rows),
        "output_csv": output_paths["csv"],
        "output_summary_json": output_paths["summary"],
        "applied_to_db": False,
    }

    if apply_to_db:
        db_result = _upsert_industry_to_supabase(mapping_rows)
        result["applied_to_db"] = True
        result.update(db_result)

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync industry mapping from Vietstock.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Upsert mapped `industry` to Supabase table `tickers`.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Sleep seconds between each top-level industry request.",
    )
    args = parser.parse_args()

    result = run_sync(apply_to_db=args.apply, sleep_seconds=max(args.sleep, 0.0))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
