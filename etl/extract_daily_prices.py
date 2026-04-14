import pandas as pd
from vnstock import stock_historical_data
from .config import get_supabase_client
import time
from datetime import datetime, timedelta, timezone # Thêm dòng import timezone

def fetch_and_store_daily_prices():
    supabase = get_supabase_client()
    if not supabase:
        return False

    response = supabase.table("tickers").select("ticker").execute()
    tickers = [item['ticker'] for item in response.data]

    # --- ĐOẠN CẦN SỬA: Xử lý múi giờ và Incremental Load ---
    vn_tz = timezone(timedelta(hours=7))
    end_date = datetime.now(vn_tz).strftime("%Y-%m-%d")
    start_date = (datetime.now(vn_tz) - timedelta(days=5)).strftime("%Y-%m-%d")
    # --------------------------------------------------------

    for i, ticker in enumerate(tickers):
        try:
            # Sử dụng stock_historical_data
            df = stock_historical_data(
                symbol=ticker, 
                start_date=start_date, # Sử dụng start_date mới (5 ngày gần nhất)
                end_date=end_date, 
                resolution='1D',
                type='stock' 
            )
            
            if df is None or df.empty:
                continue

            records = []
            for index, row in df.iterrows():
                 record = {
                     "ticker": ticker,
                     "trading_date": str(row.get('time')).split(' ')[0], 
                     "open_price": float(row.get('open', 0)),
                     "high_price": float(row.get('high', 0)),
                     "low_price": float(row.get('low', 0)),
                     "close_price": float(row.get('close', 0)),
                     "volume": int(row.get('volume', 0))
                 }
                 records.append(record)
                 
            if records:
                # Upsert với on_conflict để tránh trùng lặp ngày/mã
                supabase.table("daily_prices").upsert(records, on_conflict="ticker,trading_date").execute()
                print(f"  Upserted data for {ticker} from {start_date} to {end_date}.")
            
            time.sleep(0.5) # Tránh bị rate limit

        except Exception as e:
            print(f"  Error processing {ticker}: {e}")
            
    return True

if __name__ == "__main__":
    fetch_and_store_daily_prices()
