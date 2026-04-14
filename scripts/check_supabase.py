import sys
import os

# Ensure the root directory is in sys.path so we can import etl.config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.config import get_supabase_client

def check_tables():
    supabase = get_supabase_client()
    try:
        # Try to query the database to see what tables are available
        # We can't easily list all tables via the Supabase client without raw SQL,
        # but we can try to query 'tickers' and 'company_info' to see which one works.
        
        print("Checking for 'tickers' table...")
        try:
            res = supabase.table("tickers").select("*", count="exact").limit(1).execute()
            print(f"  Success! Found 'tickers' table. Row count: {res.count}")
        except Exception as e:
            print(f"  Failed to find 'tickers' table: {e}")
            
        print("\nChecking for 'company_info' table...")
        try:
            res = supabase.table("company_info").select("*", count="exact").limit(1).execute()
            print(f"  Success! Found 'company_info' table. Row count: {res.count}")
        except Exception as e:
            print(f"  Failed to find 'company_info' table: {e}")
            
        print("\nChecking for 'daily_prices' table...")
        try:
            res = supabase.table("daily_prices").select("*", count="exact").limit(1).execute()
            print(f"  Success! Found 'daily_prices' table. Row count: {res.count}")
        except Exception as e:
            print(f"  Failed to find 'daily_prices' table: {e}")

    except Exception as e:
        print(f"General error: {e}")

if __name__ == "__main__":
    check_tables()
