import datetime
import pandas as pd
from vnstock import Trading
from config import get_supabase_client

# Target date range for E15 (Historical 2025 and 2026 data)
START_DATE = "2025-01-01"

def get_current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def fetch_and_store_daily_prices():
    supabase = get_supabase_client()
    if not supabase:
        print("Database not configured. Exiting.")
        return False

    # First, get the list of tickers we care about from the database
    print("Retrieving tickers from database...")
    try:
        response = supabase.table("tickers").select("ticker").execute()
        tickers = [item['ticker'] for item in response.data]
    except Exception as e:
        print(f"Failed to query tickers: {e}")
        return False

    print(f"Found {len(tickers)} tickers to process for historical prices.")
    end_date = get_current_date()

    for i, ticker in enumerate(tickers):
        print(f"Processing ({i+1}/{len(tickers)}): {ticker}...")
        try:
            # fetch OHLCV data using vnstock (resolution 1D)
            # Note: vnstock automatically returns adjusted prices for historical data unless specified otherwise
            df = Trading.historical_data(
                symbol=ticker, 
                start_date=START_DATE, 
                end_date=end_date, 
                resolution='1D'
            )
            
            if df is None or df.empty:
                print(f"  No data found for {ticker}")
                continue

            # Standardize columns based on vnstock's return format (time, open, high, low, close, volume)
            records = []
            for index, row in df.iterrows():
                 record = {
                     "ticker": ticker,
                     # vnstock returns 'time' column with dates usually
                     "trading_date": str(row.get('time')).split(' ')[0], 
                     "open_price": float(row.get('open', 0)),
                     "high_price": float(row.get('high', 0)),
                     "low_price": float(row.get('low', 0)),
                     "close_price": float(row.get('close', 0)), # Adjusted close
                     "volume": int(row.get('volume', 0))
                 }
                 records.append(record)
                 
            # Batch upsert to prevent duplicate date/ticker rows (handled by unique constraint in DB)
            if records:
                # We do smaller batches if needed, but Supabase can handle reasonable sized lists
                supabase.table("daily_prices").upsert(records).execute()
                print(f"  Upserted {len(records)} days of data for {ticker}.")

        except Exception as e:
            print(f"  Error processing {ticker}: {e}")
            
    return True

if __name__ == "__main__":
    fetch_and_store_daily_prices()
