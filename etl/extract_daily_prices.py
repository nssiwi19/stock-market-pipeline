"""
extract_daily_prices.py — Trích xuất lịch sử giá giao dịch hàng ngày.

DATA SOURCE: KBS API trực tiếp (KHÔNG qua vnstock3, KHÔNG rate limit).
URL: https://kbbuddywts.kbsec.com.vn/iis-server/investment/stocks/{ticker}/data_day

ThreadPoolExecutor (max_workers=10) + Tenacity retry + Batch upsert + Dedup.
"""

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


def extract_and_upsert_stock_data():
    """Orchestrator: Quét toàn bộ tickers qua KBS API, 10 luồng song song."""
    supabase = config.get_supabase_client()

    response = supabase.table("tickers").select("ticker").execute()
    tickers = [item['ticker'] for item in response.data]

    cutoff_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')

    all_records = []
    batch_size = 50
    count = 0
    error_count = 0

    print(f"🚀 Bắt đầu cào dữ liệu giá (DIRECT KBS API) cho {len(tickers)} mã...")
    print(f"🚀 ThreadPoolExecutor(max_workers=10) — KHÔNG rate limit!")

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
                    all_records.extend(records)
            except Exception as exc:
                error_count += 1
                # Chỉ log lỗi thực sự, bỏ qua mã không có data
                if '404' not in str(exc):
                    print(f"❌ LỖI MÃ {ticker}: {exc}")

            count += 1

            if len(all_records) >= batch_size:
                batch = _dedup_records(all_records[:batch_size], ('ticker', 'trading_date'))
                all_records = all_records[batch_size:]

                try:
                    supabase.table("daily_prices").upsert(
                        batch, on_conflict="ticker,trading_date"
                    ).execute()
                    print(f"✅ Upsert lô giá. Tiến độ: {count}/{len(tickers)}")
                except Exception as e:
                    print(f"💥 Lỗi Supabase: {e}")

    # Lô cuối
    if all_records:
        batch = _dedup_records(all_records, ('ticker', 'trading_date'))
        try:
            supabase.table("daily_prices").upsert(
                batch, on_conflict="ticker,trading_date"
            ).execute()
            print(f"🏁 Nạp nốt {len(batch)} bản ghi cuối.")
        except Exception as e:
            print(f"💥 Lỗi Supabase lô cuối: {e}")

    success_rate = ((count - error_count) / count * 100) if count > 0 else 0
    print(f"🏁 Hoàn tất. Thành công: {count - error_count}/{count} ({success_rate:.1f}%)")


if __name__ == "__main__":
    extract_and_upsert_stock_data()