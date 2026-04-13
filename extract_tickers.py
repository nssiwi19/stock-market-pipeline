import pandas as pd
from vnstock import Listing
from config import get_supabase_client

# Note: We filter for HOSE and HNX. UPCOM can be added if needed.
# vnstock's Listing module is used here. For v3.x, Listing.all_symbols() is common.
# If using an older version, the functions might be slightly different. We assume v3+ syntax based on recent docs.

def fetch_and_store_tickers():
    print("Fetching active tickers from the market...")
    try:
        # Get all symbols
        df_symbols = Listing.all_symbols()
        
        # Filter for HOSE and HNX
        target_exchanges = ['HOSE', 'HNX']
        df_filtered = df_symbols[df_symbols['exchange'].isin(target_exchanges)]
        
        # Select and rename columns to match our database schema
        # In vnstock, common columns are 'ticker', 'exchange', 'industry_en', 'company_name'
        # We handle potential schema differences gracefully
        
        records_to_insert = []
        for index, row in df_filtered.iterrows():
            # Handle potential None or missing columns
            ticker = row.get('ticker')
            exchange = row.get('exchange')
            industry = row.get('industry', 'N/A') # fallback
            company_name = row.get('company_name', row.get('short_name', 'N/A'))
            
            records_to_insert.append({
                "ticker": ticker,
                "exchange": exchange,
                "industry": str(industry),
                "company_name": str(company_name)
            })
            
        print(f"Found {len(records_to_insert)} tickers for HOSE & HNX.")
        
        # Insert to Supabase
        supabase = get_supabase_client()
        if supabase:
            # Upsert using 'ticker' as the unique key
            # Supabase Python client 'upsert' works when the primary key is provided
            response = supabase.table("tickers").upsert(records_to_insert).execute()
            print("Successfully populated the `tickers` table.")
            return True
        else:
             print("Database not configured. Cannot save tickers.")
             return False
             
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return False

if __name__ == "__main__":
    fetch_and_store_tickers()
