"""
extract_financials.py — Trích xuất Báo cáo Tài chính từ Cafef.

DATA SOURCE: Cafef HTML pages (confirmed working 2026-04-16).
URLs:
  - Income Statement: /bao-cao-tai-chinh/{ticker}/IncSta/0/0/0/0/...
  - Balance Sheet:    /bao-cao-tai-chinh/{ticker}/BSheet/0/0/0/0/...

Mỗi trang trả về 3-4 năm dữ liệu trong 1 request duy nhất.
ThreadPoolExecutor (max_workers=10) + Tenacity retry + Batch upsert.
"""

import json
import os
import time
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from etl import config
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html, */*',
}

CAFEF_INCSTA_URL = "https://s.cafef.vn/bao-cao-tai-chinh/{ticker}/IncSta/{year}/0/0/0/ket-qua-hoat-dong-kinh-doanh-.chn"
CAFEF_BSHEET_URL = "https://s.cafef.vn/bao-cao-tai-chinh/{ticker}/BSheet/{year}/0/0/0/bang-can-doi-ke-toan-.chn"


def _parse_vn_number(text: str) -> float:
    """Parse số tiền Việt Nam: '60.074.730.223.299' → 60074730223299.0"""
    if not text or text.strip() in ('-', '', 'N/A', '--', '0'):
        return 0.0
    text = text.strip()
    negative = text.startswith('-')
    if negative:
        text = text[1:]
    # Cafef dùng dấu chấm ngăn hàng nghìn
    text = text.replace('.', '').replace(',', '').strip()
    try:
        val = float(text)
        return -val if negative else val
    except ValueError:
        return 0.0


def _find_financial_table(soup: BeautifulSoup):
    """Tìm bảng DATA tài chính (bảng có nhiều <tr> nhất chứa keyword)."""
    best_table = None
    best_rows = 0
    for table in soup.find_all('table'):
        text = table.get_text().lower()
        if 'doanh thu' in text or 'tổng cộng tài sản' in text or 'lợi nhuận' in text:
            num_rows = len(table.find_all('tr'))
            if num_rows > best_rows:
                best_rows = num_rows
                best_table = table
    return best_table


def _extract_row_value(rows, keyword: str, col_idx: int = 1) -> float:
    """Tìm row chứa keyword và trả về giá trị ở cột col_idx."""
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).lower()
            if keyword in label:
                if col_idx < len(cells):
                    return _parse_vn_number(cells[col_idx].get_text(strip=True))
    return 0.0


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    reraise=True
)
def fetch_single_ticker_financials(ticker: str) -> list[dict]:
    """
    Scrape Income Statement + Balance Sheet từ Cafef cho 1 mã.
    Mỗi trang trả về 3 cột dữ liệu (3 năm gần nhất).
    Chỉ cần 2 HTTP requests/mã.
    """
    current_year = datetime.now().year
    extracted_records = []

    # === Lấy Income Statement (năm hiện tại → data 3 năm gần nhất) ===
    url_inc = CAFEF_INCSTA_URL.format(ticker=ticker, year=current_year)
    resp_inc = requests.get(url_inc, headers=HEADERS, timeout=15)
    if resp_inc.status_code != 200:
        return []

    soup_inc = BeautifulSoup(resp_inc.text, 'html.parser')
    table_inc = _find_financial_table(soup_inc)

    if not table_inc:
        return []

    rows_inc = table_inc.find_all('tr')

    # === Lấy Balance Sheet ===
    url_bs = CAFEF_BSHEET_URL.format(ticker=ticker, year=current_year)
    resp_bs = requests.get(url_bs, headers=HEADERS, timeout=15)

    rows_bs = []
    if resp_bs.status_code == 200:
        soup_bs = BeautifulSoup(resp_bs.text, 'html.parser')
        table_bs = _find_financial_table(soup_bs)
        if table_bs:
            rows_bs = table_bs.find_all('tr')

    # === Parse 3 cột (3 năm gần nhất) ===
    for col_idx in range(1, 4):  # Cột 1, 2, 3
        year = current_year - (col_idx - 1) - 1  # 2025, 2024, 2023
        period_str = f"FY-{year}"

        # Income Statement
        revenue = _extract_row_value(rows_inc, 'doanh thu thuần', col_idx) / 1e9  # → tỷ VND
        profit = _extract_row_value(rows_inc, 'lợi nhuận sau thuế', col_idx) / 1e9

        # Balance Sheet
        total_assets = _extract_row_value(rows_bs, 'tổng cộng tài sản', col_idx) / 1e9 if rows_bs else 0
        total_liabilities = _extract_row_value(rows_bs, 'nợ phải trả', col_idx) / 1e9 if rows_bs else 0
        equity = _extract_row_value(rows_bs, 'vốn chủ sở hữu', col_idx) / 1e9 if rows_bs else 0

        if revenue != 0 or total_assets != 0:
            extracted_records.append({
                "ticker": ticker,
                "report_type": "yearly",
                "period": period_str,
                "revenue": revenue,
                "profit_after_tax": profit,
                "total_assets": total_assets,
                "total_liabilities": total_liabilities,
                "equity": equity,
                "eps": 0
            })

    return extracted_records


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
                print(f"⚠️ Upsert {table_name} lỗi (attempt {attempt}/{max_attempts}), retry sau {sleep_s}s: {exc}")
                time.sleep(sleep_s)
    return False, last_error


def fetch_and_store_financials():
    """Orchestrator: Quét toàn bộ tickers, scrape Cafef đa luồng, batch upsert."""
    supabase = config.get_supabase_client()

    response = supabase.table("tickers").select("ticker").execute()
    tickers = [item['ticker'] for item in response.data]
    if not tickers:
        return {
            "step": "extract_financials",
            "success": False,
            "records_fetched": 0,
            "records_upserted": 0,
            "errors": 1,
            "error_rate": 1.0,
            "failed_batches": 0,
            "fail_fast_triggered": False,
            "message": "Không có ticker nào trong bảng tickers.",
        }

    all_financials = []
    batch_size = 30
    count = 0
    error_count = 0
    records_fetched = 0
    records_upserted = 0
    failed_batches = 0
    fail_fast_triggered = False
    max_error_rate = float(os.getenv("PIPELINE_MAX_ERROR_RATE", "0.25"))
    min_processed_before_fail_fast = int(os.getenv("PIPELINE_MIN_ITEMS_FOR_FAIL_FAST", "20"))

    print(f"📊 Bắt đầu cào BCTC (Cafef IncSta + BSheet) cho {len(tickers)} mã...")
    print(f"🚀 ThreadPoolExecutor(max_workers=10) — 2 req/mã, ~{len(tickers)*2//10}s ETA")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {
            executor.submit(fetch_single_ticker_financials, ticker): ticker
            for ticker in tickers
        }

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                records = future.result()
                if records:
                    records_fetched += len(records)
                    all_financials.extend(records)
            except Exception as exc:
                error_count += 1
                print(f"❌ LỖI MÃ {ticker}: {exc}")

            count += 1
            current_error_rate = (error_count / count) if count > 0 else 0
            if count >= min_processed_before_fail_fast and current_error_rate > max_error_rate:
                fail_fast_triggered = True
                print(
                    f"🛑 FAIL-FAST: Tỷ lệ lỗi {current_error_rate:.1%} vượt ngưỡng {max_error_rate:.1%} "
                    f"sau {count} mã."
                )
                break

            if len(all_financials) >= batch_size:
                batch = all_financials[:batch_size]
                success, err = _upsert_with_retry(
                    supabase=supabase,
                    table_name="financial_reports",
                    batch=batch,
                    on_conflict="ticker,report_type,period",
                    max_attempts=3,
                )
                if success:
                    records_upserted += len(batch)
                    print(f"✅ Upsert lô BCTC. Tiến độ: {count}/{len(tickers)} | Records: {len(batch)}")
                else:
                    failed_batches += 1
                    path = _persist_failed_batch("financial_reports", batch, err)
                    print(f"💥 Lỗi Supabase sau retry. Đã lưu dead-letter: {path}")
                all_financials = all_financials[batch_size:]

    if all_financials:
        success, err = _upsert_with_retry(
            supabase=supabase,
            table_name="financial_reports",
            batch=all_financials,
            on_conflict="ticker,report_type,period",
            max_attempts=3,
        )
        if success:
            records_upserted += len(all_financials)
            print(f"🏁 Nạp nốt {len(all_financials)} bản ghi cuối.")
        else:
            failed_batches += 1
            path = _persist_failed_batch("financial_reports", all_financials, err)
            print(f"💥 Lỗi Supabase lô cuối sau retry. Đã lưu dead-letter: {path}")

    success_rate = ((count - error_count) / count * 100) if count > 0 else 0
    print(f"🏁 Hoàn tất. Thành công: {count - error_count}/{count} ({success_rate:.1f}%)")
    print(
        f"📦 Records fetched={records_fetched}, upserted={records_upserted}, "
        f"failed_batches={failed_batches}"
    )

    final_error_rate = (error_count / count) if count > 0 else 1.0
    is_success = (
        not fail_fast_triggered
        and failed_batches == 0
        and final_error_rate <= max_error_rate
    )
    return {
        "step": "extract_financials",
        "success": is_success,
        "records_fetched": records_fetched,
        "records_upserted": records_upserted,
        "errors": error_count,
        "error_rate": round(final_error_rate, 4),
        "failed_batches": failed_batches,
        "fail_fast_triggered": fail_fast_triggered,
        "max_error_rate": max_error_rate,
    }


if __name__ == "__main__":
    fetch_and_store_financials()
