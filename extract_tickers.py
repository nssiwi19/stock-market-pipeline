import pandas as pd
from vnstock import listing_companies # Sửa lại import
from config import get_supabase_client

def fetch_and_store_tickers():
    print("Fetching active tickers from the market...")
    try:
        # Sử dụng hàm trực tiếp thay cho Listing.all_symbols()
        df_symbols = listing_companies()
        
        # Trong v0.2.x, cột sàn thường là 'comGroupCode'
        target_exchanges = ['HOSE', 'HNX']
        df_filtered = df_symbols[df_symbols['comGroupCode'].isin(target_exchanges)]
        
        records_to_insert = []
        for index, row in df_filtered.iterrows():
            ticker = row.get('ticker')
            exchange = row.get('comGroupCode')
            industry = row.get('icbName3', 'N/A') 
            company_name = row.get('organName', 'N/A')
            
            records_to_insert.append({
                "ticker": ticker,
                "exchange": exchange,
                "industry": str(industry),
                "company_name": str(company_name)
            })
            
        print(f"Found {len(records_to_insert)} tickers for HOSE & HNX.")
        
        supabase = get_supabase_client()
        if supabase:
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