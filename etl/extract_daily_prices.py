"""
extract_daily_prices.py — Trích xuất lịch sử giá giao dịch hàng ngày.

DATA SOURCE: KBS API trực tiếp (KHÔNG qua vnstock3, KHÔNG rate limit).
URL: https://kbbuddywts.kbsec.com.vn/iis-server/investment/stocks/{ticker}/data_day

ThreadPoolExecutor (max_workers=10) + Tenacity retry + Batch upsert + Dedup.
"""

import json
import os
import time
from pathlib import Path
import requests
from datetime import datetime, timedelta
from etl import config
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class PermanentError(Exception):
    """Lỗi vĩnh viễn (404, mã không tồn tại) — KHÔNG retry."""
    pass


KBS_BASE_URL = "https://kbbuddywts.kbsec.com.vn/iis-server/investment"

KBS_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
}


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    reraise=True
)
def fetch_single_ticker_prices(ticker: str, cutoff_date: str) -> list[dict]:
    """
    Gọi trực tiếp KBS API để lấy OHLCV daily.
    KBS trả về ~100 ngày gần nhất, ta lọc theo cutoff_date.
    Giá KBS trả về đơn vị VND, chia 1000 để khớp format vnstock cũ.
    """
    url = f"{KBS_BASE_URL}/stocks/{ticker}/data_day"
    response = requests.get(url, headers=KBS_HEADERS, timeout=15)

    if response.status_code == 404:
        raise PermanentError(f"Mã {ticker} không tồn tại trên KBS")
    if response.status_code != 200:
        raise requests.exceptions.ConnectionError(f"HTTP {response.status_code}")

    data = response.json()
    bars = data.get('data_day', [])

    if not bars:
        return []

    extracted_records = []
    for bar in bars:
        t = bar.get('t', '')
        if not t:
            continue

        # Parse date: "2026-04-15 07:00" → "2026-04-15"
        trading_date = t[:10]

        # Chỉ lấy data từ cutoff_date trở đi
        if trading_date < cutoff_date:
            continue

        extracted_records.append({
            "ticker": ticker,
            "trading_date": trading_date,
            "open_price": float(bar.get('o', 0)) / 1000,
            "high_price": float(bar.get('h', 0)) / 1000,
            "low_price": float(bar.get('l', 0)) / 1000,
            "close_price": float(bar.get('c', 0)) / 1000,
            "volume": int(bar.get('v', 0))
        })

    return extracted_records


def _dedup_records(records: list[dict], keys: tuple) -> list[dict]:
    """Loại bỏ bản ghi trùng composite key trong 1 batch."""
    seen = {}
    for r in records:
        k = tuple(r[col] for col in keys)
        seen[k] = r
    return list(seen.values())


def _persist_failed_batch(table_name: str, batch: list[dict], error: Exception) -> str:
    """Ghi batch lỗi ra dead-letter để có thể replay thủ công."""
    dead_letter_dir = Path(__file__).resolve().parent / "dead_letter"
    dead_letter_dir.mkdir(parents=True, exist_ok=True)
    file_path = dead_letter_dir / f"{table_name}_failed_batches.jsonl"

    payload = {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "table": table_name,
        "error": str(error),
        "batch_size": len(batch),
        "records": batch,
    }
    with file_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return str(file_path)


def _is_price_logic_valid(record: dict) -> bool:
    """
    Validate logic OHLC cơ bản để tránh vi phạm check_price_logic ở DB.
    Rule tối thiểu:
      - low <= high
      - low <= open/close <= high
      - volume >= 0
    """
    try:
        o = float(record["open_price"])
        h = float(record["high_price"])
        l = float(record["low_price"])
        c = float(record["close_price"])
        v = int(record["volume"])
    except (KeyError, TypeError, ValueError):
        return False

    if l > h:
        return False
    if not (l <= o <= h):
        return False
    if not (l <= c <= h):
        return False
    if v < 0:
        return False
    return True


def _split_valid_invalid_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Tách records hợp lệ và records vi phạm price logic."""
    valid_records = []
    invalid_records = []
    for r in records:
        if _is_price_logic_valid(r):
            valid_records.append(r)
        else:
            invalid_records.append(r)
    return valid_records, invalid_records


def _upsert_with_retry(supabase, table_name: str, batch: list[dict], on_conflict: str, max_attempts: int = 3):
    """Upsert có retry; trả về tuple (success, error)."""
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            supabase.table(table_name).upsert(batch, on_conflict=on_conflict).execute()
            return True, None
        except Exception as exc:
            last_error = exc
            if attempt < max_attempts:
                sleep_s = 2 ** (attempt - 1)
                print(f"[WARN] Upsert {table_name} failed (attempt {attempt}/{max_attempts}), retry in {sleep_s}s: {exc}")
                time.sleep(sleep_s)
    return False, last_error


def extract_and_upsert_stock_data():
    """Orchestrator: Quét toàn bộ tickers qua KBS API, 10 luồng song song."""
    supabase = config.get_supabase_client()

    response = supabase.table("tickers").select("ticker").execute()
    tickers = [item['ticker'] for item in response.data]
    if not tickers:
        return {
            "step": "extract_daily_prices",
            "success": False,
            "records_fetched": 0,
            "records_upserted": 0,
            "errors": 1,
            "error_rate": 1.0,
            "failed_batches": 0,
            "fail_fast_triggered": False,
            "message": "Không có ticker nào trong bảng tickers.",
        }

    cutoff_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    max_error_rate = float(os.getenv("PIPELINE_MAX_ERROR_RATE", "0.25"))
    min_processed_before_fail_fast = int(os.getenv("PIPELINE_MIN_ITEMS_FOR_FAIL_FAST", "20"))

    all_records = []
    batch_size = 50
    count = 0
    error_count = 0
    not_found_count = 0
    records_fetched = 0
    records_upserted = 0
    rejected_records = 0
    failed_batches = 0
    fail_fast_triggered = False

    print(f"[INFO] Start fetching daily prices (DIRECT KBS API) for {len(tickers)} tickers...")
    print("[INFO] ThreadPoolExecutor(max_workers=10) - no rate limit.")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {
            executor.submit(fetch_single_ticker_prices, ticker, cutoff_date): ticker
            for ticker in tickers
        }

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                records = future.result()
                if records:
                    records_fetched += len(records)
                    all_records.extend(records)
            except PermanentError:
                not_found_count += 1
            except Exception as exc:
                error_count += 1
                print(f"[ERROR] Ticker {ticker} failed: {exc}")

            count += 1
            current_error_rate = (error_count / count) if count > 0 else 0
            if count >= min_processed_before_fail_fast and current_error_rate > max_error_rate:
                fail_fast_triggered = True
                print(
                    f"[FAIL-FAST] Error rate {current_error_rate:.1%} exceeded threshold {max_error_rate:.1%} "
                    f"after {count} tickers."
                )
                break

            if len(all_records) >= batch_size:
                batch = _dedup_records(all_records[:batch_size], ('ticker', 'trading_date'))
                valid_batch, invalid_batch = _split_valid_invalid_records(batch)
                if invalid_batch:
                    rejected_records += len(invalid_batch)
                    path = _persist_failed_batch(
                        "daily_prices_invalid_rows",
                        invalid_batch,
                        Exception("Vi phạm check_price_logic (pre-check)"),
                    )
                    print(
                        f"[WARN] Rejected {len(invalid_batch)} rows violating price logic. "
                        f"Dead-letter saved: {path}"
                    )

                if not valid_batch:
                    all_records = all_records[batch_size:]
                    continue

                success, err = _upsert_with_retry(
                    supabase=supabase,
                    table_name="daily_prices",
                    batch=valid_batch,
                    on_conflict="ticker,trading_date",
                    max_attempts=3,
                )
                if success:
                    records_upserted += len(valid_batch)
                    print(f"[OK] Daily price batch upserted. Progress: {count}/{len(tickers)}")
                else:
                    failed_batches += 1
                    path = _persist_failed_batch("daily_prices", valid_batch, err)
                    print(f"[ERROR] Supabase upsert failed after retries. Dead-letter saved: {path}")
                all_records = all_records[batch_size:]

    # Lô cuối
    if all_records:
        batch = _dedup_records(all_records, ('ticker', 'trading_date'))
        valid_batch, invalid_batch = _split_valid_invalid_records(batch)
        if invalid_batch:
            rejected_records += len(invalid_batch)
            path = _persist_failed_batch(
                "daily_prices_invalid_rows",
                invalid_batch,
                Exception("Vi phạm check_price_logic (pre-check)"),
            )
            print(
                f"[WARN] Rejected {len(invalid_batch)} rows violating price logic in final batch. "
                f"Dead-letter saved: {path}"
            )

        if valid_batch:
            success, err = _upsert_with_retry(
                supabase=supabase,
                table_name="daily_prices",
                batch=valid_batch,
                on_conflict="ticker,trading_date",
                max_attempts=3,
            )
            if success:
                records_upserted += len(valid_batch)
                print(f"[OK] Final batch upserted with {len(valid_batch)} records.")
            else:
                failed_batches += 1
                path = _persist_failed_batch("daily_prices", valid_batch, err)
                print(f"[ERROR] Final Supabase upsert failed after retries. Dead-letter saved: {path}")

    success_rate = ((count - error_count) / count * 100) if count > 0 else 0
    print(f"[DONE] Completed. Success: {count - error_count}/{count} ({success_rate:.1f}%)")
    print(
        f"[STATS] Records fetched={records_fetched}, upserted={records_upserted}, "
        f"rejected_records={rejected_records}, failed_batches={failed_batches}, not_found={not_found_count}"
    )

    final_error_rate = (error_count / count) if count > 0 else 1.0
    rejected_rate = (rejected_records / records_fetched) if records_fetched > 0 else 0.0
    max_rejected_rate = float(os.getenv("PIPELINE_MAX_REJECTED_RATE", "0.03"))
    is_success = (
        not fail_fast_triggered
        and failed_batches == 0
        and final_error_rate <= max_error_rate
        and rejected_rate <= max_rejected_rate
    )
    return {
        "step": "extract_daily_prices",
        "success": is_success,
        "records_fetched": records_fetched,
        "records_upserted": records_upserted,
        "rejected_records": rejected_records,
        "rejected_rate": round(rejected_rate, 4),
        "errors": error_count,
        "error_rate": round(final_error_rate, 4),
        "failed_batches": failed_batches,
        "not_found": not_found_count,
        "fail_fast_triggered": fail_fast_triggered,
        "max_error_rate": max_error_rate,
        "max_rejected_rate": max_rejected_rate,
    }


if __name__ == "__main__":
    extract_and_upsert_stock_data()
