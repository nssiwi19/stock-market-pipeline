import datetime
import pandas as pd
from vnstock import stock_historical_data # Sửa lại import
from config import get_supabase_client
import time

START_DATE = "2025-01-01"

def fetch_and_store_daily_prices():
    supabase = get_supabase_client()
    if not supabase:
        return False

    response = supabase.table("tickers").select("ticker").execute()
    tickers = [item['ticker'] for item in response.data]

    end_date = datetime.datetime.now().strftime("%Y-%m-%d")

    for i, ticker in enumerate(tickers):
        try:
            # Sử dụng stock_historical_data thay cho Trading.historical_data
            df = stock_historical_data(
                symbol=ticker, 
                start_date=START_DATE, 
                end_date=end_date, 
                resolution='1D',
                type='stock' # Thêm type cho chắc chắn
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
                supabase.table("daily_prices").upsert(records).execute()
                print(f"  Upserted data for {ticker}.")
            
            time.sleep(0.5) # Tránh bị rate limit

        except Exception as e:
            print(f"  Error processing {ticker}: {e}")
            
    return True