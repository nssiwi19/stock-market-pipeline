import pandas as pd
from vnstock import listing_companies
from config import get_supabase_client

def fetch_and_store_tickers():
    print("Fetching active tickers from the market...")
    try:
        # Get all symbols using old API
        df_symbols = listing_companies()
        
        # In old vnstock, exchange column is 'comGroupCode'
        target_exchanges = ['HOSE', 'HNX']
        if 'comGroupCode' in df_symbols.columns:
            df_filtered = df_symbols[df_symbols['comGroupCode'].isin(target_exchanges)]
        else:
            df_filtered = df_symbols # fallback if API changed
        
        records_to_insert = []
        for index, row in df_filtered.iterrows():
            ticker = row.get('ticker')
            exchange = row.get('comGroupCode', 'UNKNOWN')
            industry = row.get('icbName3', 'N/A') 
            company_name = row.get('organName', row.get('organShortName', 'N/A'))
            
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
        else:
             print("Database not configured. Cannot save tickers.")
             
    except Exception as e:
        print(f"Error fetching tickers: {e}")

if __name__ == "__main__":
    fetch_and_store_tickers()
