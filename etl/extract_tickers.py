"""
extract_tickers.py — Trích xuất danh sách mã chứng khoán.

DATA SOURCE: vnstock3 Listing (KBS) — confirmed working 2026-04-16.
"""

from vnstock import Listing
from .config import get_supabase_client


def fetch_and_store_tickers():
    """Lấy danh sách mã HOSE + HNX từ vnstock3 và upsert vào Supabase."""
    print("Fetching active tickers from the market (vnstock3 Listing)...")
    try:
        listing = Listing()
        df_symbols = listing.all_symbols()

        # vnstock3 trả về cột: symbol, organ_name, ...
        # Lọc sàn HOSE và HNX
        target_exchanges = ['HOSE', 'HNX']
        if 'comGroupCode' in df_symbols.columns:
            df_filtered = df_symbols[df_symbols['comGroupCode'].isin(target_exchanges)]
        elif 'exchange' in df_symbols.columns:
            df_filtered = df_symbols[df_symbols['exchange'].isin(target_exchanges)]
        else:
            # Nếu vnstock3 không có cột exchange, lấy tất cả
            df_filtered = df_symbols
            print(f"  ⚠️ Không tìm thấy cột exchange. Columns: {list(df_symbols.columns)}")

        records_to_insert = []
        for _, row in df_filtered.iterrows():
            ticker = row.get('symbol', row.get('ticker', ''))
            exchange = row.get('comGroupCode', row.get('exchange', 'N/A'))
            industry = row.get('icbName3', row.get('industry', 'N/A'))
            company_name = row.get('organ_name', row.get('organName', row.get('company_name', 'N/A')))

            if ticker:
                records_to_insert.append({
                    "ticker": str(ticker),
                    "exchange": str(exchange) if exchange else 'N/A',
                    "industry": str(industry) if industry else 'N/A',
                    "company_name": str(company_name) if company_name else 'N/A'
                })

        print(f"Found {len(records_to_insert)} tickers.")

        supabase = get_supabase_client()
        if supabase:
            # Upsert theo batch để tránh payload quá lớn
            batch_size = 100
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i + batch_size]
                supabase.table("tickers").upsert(batch).execute()
            print("Successfully populated the `tickers` table.")
            return True
        else:
            print("Database not configured. Cannot save tickers.")
            return False

    except Exception as e:
        print(f"Error fetching tickers: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    fetch_and_store_tickers()
