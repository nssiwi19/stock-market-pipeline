import sys
import os

# Ensure the root directory is in sys.path so we can import etl.config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.config import get_supabase_client

def check_tables():
    supabase = get_supabase_client()
    try:
        tables = ['tickers', 'financial_reports', 'daily_prices']
        for table_name in tables:
            print(f"\nChecking for '{table_name}' table...")
            try:
                res = supabase.table(table_name).select("*", count="exact").limit(1).execute()
                print(f"  Success! Found '{table_name}' table. Row count: {res.count}")
            except Exception as e:
                print(f"  Failed to find '{table_name}' table: {e}")

    except Exception as e:
        print(f"General error: {e}")

if __name__ == "__main__":
    check_tables()
