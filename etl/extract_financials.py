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
import re
import time
import unicodedata
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
CAFEF_CFLOW_URL = "https://s.cafef.vn/bao-cao-tai-chinh/{ticker}/CashFlow/{year}/0/0/0/luu-chuyen-tien-te-gian-tiep-.chn"


def _normalize_text(text: str) -> str:
    """Lowercase + bỏ dấu tiếng Việt để match keyword ổn định."""
    if text is None:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.lower().strip().split())


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
        text = _normalize_text(table.get_text())
        if (
            'doanh thu' in text
            or 'tong cong tai san' in text
            or 'loi nhuan' in text
            or 'luu chuyen tien te' in text
        ):
            num_rows = len(table.find_all('tr'))
            if num_rows > best_rows:
                best_rows = num_rows
                best_table = table
    return best_table


def _extract_row_value(rows, keywords: tuple[str, ...], col_idx: int = 1, scale: float = 1e9) -> float:
    """Tìm row chứa 1 trong các keyword và trả về giá trị ở cột col_idx."""
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 2:
            label = _normalize_text(cells[0].get_text(strip=True))
            if any(keyword in label for keyword in keywords):
                if col_idx < len(cells):
                    return _parse_vn_number(cells[col_idx].get_text(strip=True)) / scale
    return 0.0


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator in (0, 0.0):
        return 0.0
    return numerator / denominator


def _extract_period(rows, col_idx: int, fallback_year: int) -> str:
    """Lấy năm từ header cột; fallback về FY-{fallback_year}."""
    for row in rows[:5]:
        cells = row.find_all('td')
        if col_idx < len(cells):
            text = cells[col_idx].get_text(" ", strip=True)
            match = re.search(r"(20\d{2})", text)
            if match:
                return f"FY-{match.group(1)}"
    return f"FY-{fallback_year}"


INCOME_METRICS = {
    "revenue": (("doanh thu thuan",), 1e9),
    "cogs": (("gia von hang ban",), 1e9),
    "gross_profit": (("loi nhuan gop",), 1e9),
    "financial_income": (("doanh thu hoat dong tai chinh",), 1e9),
    "financial_expense": (("chi phi tai chinh",), 1e9),
    "interest_expense": (("chi phi lai vay",), 1e9),
    "selling_expense": (("chi phi ban hang",), 1e9),
    "general_admin_expense": (("chi phi quan ly doanh nghiep",), 1e9),
    "operating_profit": (("loi nhuan thuan tu hoat dong kinh doanh",), 1e9),
    "other_income": (("thu nhap khac",), 1e9),
    "other_expense": (("chi phi khac",), 1e9),
    "profit_before_tax": (("tong loi nhuan ke toan truoc thue",), 1e9),
    "profit_after_tax": (("loi nhuan sau thue thu nhap doanh nghiep",), 1e9),
    "parent_profit_after_tax": (("loi nhuan sau thue cua cong ty me",), 1e9),
    "minority_profit": (("loi ich cua co dong khong kiem soat",), 1e9),
    "depreciation_amortization": (("khau hao tai san co dinh",), 1e9),
    "eps": (("lai co ban tren co phieu", "eps"), 1.0),
}

BALANCE_METRICS = {
    "cash_and_cash_equivalents": (("tien va tuong duong tien",), 1e9),
    "short_term_investments": (("dau tu tai chinh ngan han",), 1e9),
    "short_term_receivables": (("cac khoan phai thu ngan han",), 1e9),
    "inventory": (("hang ton kho",), 1e9),
    "other_current_assets": (("tai san ngan han khac",), 1e9),
    "total_current_assets": (("tong tai san ngan han",), 1e9),
    "long_term_receivables": (("phai thu dai han",), 1e9),
    "fixed_assets": (("tai san co dinh",), 1e9),
    "investment_properties": (("bat dong san dau tu",), 1e9),
    "long_term_assets": (("tai san dai han",), 1e9),
    "total_assets": (("tong cong tai san",), 1e9),
    "short_term_debt": (("vay va no thue tai chinh ngan han", "vay ngan han"), 1e9),
    "accounts_payable": (("phai tra nguoi ban",), 1e9),
    "short_term_liabilities": (("no ngan han",), 1e9),
    "total_short_term_liabilities": (("tong no ngan han",), 1e9),
    "long_term_debt": (("vay va no thue tai chinh dai han", "vay dai han"), 1e9),
    "total_long_term_liabilities": (("no dai han",), 1e9),
    "total_liabilities": (("no phai tra",), 1e9),
    "owner_equity": (("von chu so huu",), 1e9),
    "retained_earnings": (("loi nhuan sau thue chua phan phoi",), 1e9),
    "share_capital": (("von gop cua chu so huu",), 1e9),
    "total_equity_and_liabilities": (("tong cong nguon von",), 1e9),
}

CASHFLOW_METRICS = {
    "cash_flow_operating": (("luu chuyen tien thuan tu hoat dong kinh doanh",), 1e9),
    "cash_flow_investing": (("luu chuyen tien thuan tu hoat dong dau tu",), 1e9),
    "cash_flow_financing": (("luu chuyen tien thuan tu hoat dong tai chinh",), 1e9),
    "net_cash_flow": (("luu chuyen tien thuan trong ky",), 1e9),
    "capex": (("tien chi de mua sam xay dung tscd", "chi mua sam xay dung tai san co dinh"), 1e9),
}


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

    def _fetch_rows(url: str) -> list:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = _find_financial_table(soup)
        if not table:
            return []
        return table.find_all('tr')

    rows_inc = _fetch_rows(CAFEF_INCSTA_URL.format(ticker=ticker, year=current_year))
    if not rows_inc:
        return []

    rows_bs = _fetch_rows(CAFEF_BSHEET_URL.format(ticker=ticker, year=current_year))
    rows_cf = _fetch_rows(CAFEF_CFLOW_URL.format(ticker=ticker, year=current_year))

    # Cafef thường trả 3-4 cột năm gần nhất.
    max_cols = 4
    for col_idx in range(1, max_cols + 1):
        period_str = _extract_period(rows_inc, col_idx, current_year - col_idx)
        record = {
            "ticker": ticker,
            "report_type": "yearly",
            "period": period_str,
        }

        for field, (keywords, scale) in INCOME_METRICS.items():
            record[field] = _extract_row_value(rows_inc, keywords, col_idx, scale)
        for field, (keywords, scale) in BALANCE_METRICS.items():
            record[field] = _extract_row_value(rows_bs, keywords, col_idx, scale) if rows_bs else 0.0
        for field, (keywords, scale) in CASHFLOW_METRICS.items():
            record[field] = _extract_row_value(rows_cf, keywords, col_idx, scale) if rows_cf else 0.0

        record["equity"] = record.get("owner_equity", 0.0)
        record["ebit"] = record.get("operating_profit", 0.0) + record.get("interest_expense", 0.0)
        record["ebitda"] = record.get("ebit", 0.0) + record.get("depreciation_amortization", 0.0)
        record["gross_margin"] = _safe_div(record.get("gross_profit", 0.0), record.get("revenue", 0.0))
        record["operating_margin"] = _safe_div(record.get("operating_profit", 0.0), record.get("revenue", 0.0))
        record["net_margin"] = _safe_div(record.get("profit_after_tax", 0.0), record.get("revenue", 0.0))
        record["roe"] = _safe_div(record.get("profit_after_tax", 0.0), record.get("owner_equity", 0.0))
        record["roa"] = _safe_div(record.get("profit_after_tax", 0.0), record.get("total_assets", 0.0))
        record["debt_to_equity"] = _safe_div(record.get("total_liabilities", 0.0), record.get("owner_equity", 0.0))
        record["current_ratio"] = _safe_div(
            record.get("total_current_assets", 0.0),
            record.get("total_short_term_liabilities", 0.0),
        )
        record["asset_turnover"] = _safe_div(record.get("revenue", 0.0), record.get("total_assets", 0.0))

        if record.get("revenue", 0.0) != 0 or record.get("total_assets", 0.0) != 0:
            extracted_records.append(record)

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
