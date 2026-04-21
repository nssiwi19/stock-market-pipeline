"""
extract_tickers.py — Trích xuất danh sách mã chứng khoán.

DATA SOURCE: vnstock3 Listing (KBS) — confirmed working 2026-04-16.
"""

from vnstock import Listing
from .config import get_supabase_client


def _clean_text(value, fallback: str = "N/A") -> str:
    """Chuẩn hóa giá trị text, tránh lưu 'nan'/'none' dạng chuỗi."""
    if value is None:
        return fallback
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "<na>", "n/a", "unknown"}:
        return fallback
    return text


def _clean_nullable_text(value):
    text = _clean_text(value, fallback="")
    return text if text else None


def _fetch_all_existing_tickers(supabase, page_size: int = 1000) -> list[dict]:
    """Lấy toàn bộ tickers hiện có bằng pagination (Supabase mặc định limit 1000)."""
    all_rows = []
    offset = 0
    while True:
        res = (
            supabase.table("tickers")
            .select("ticker,industry,exchange,company_name")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = res.data or []
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_rows


def fetch_and_store_tickers():
    """Lấy danh sách mã HOSE + HNX từ vnstock3 và upsert vào Supabase."""
    print("Fetching active tickers from the market (vnstock3 Listing)...")
    try:
        listing = Listing()
        df_symbols = listing.all_symbols()

        # vnstock3 trả về cột: symbol, organ_name, ...
        # Lọc sàn HOSE/HNX/UPCOM để tránh hụt universe thị trường Việt Nam.
        target_exchanges = ['HOSE', 'HNX', 'UPCOM']
        if 'comGroupCode' in df_symbols.columns:
            df_filtered = df_symbols[df_symbols['comGroupCode'].isin(target_exchanges)]
        elif 'exchange' in df_symbols.columns:
            df_filtered = df_symbols[df_symbols['exchange'].isin(target_exchanges)]
        else:
            # Nếu vnstock3 không có cột exchange, lấy tất cả
            df_filtered = df_symbols
            print(f"  [WARN] exchange column not found. Columns: {list(df_symbols.columns)}")

        supabase = get_supabase_client()
        existing_rows = _fetch_all_existing_tickers(supabase)
        existing_map = {row["ticker"]: row for row in existing_rows if row.get("ticker")}

        records_to_insert = []
        for _, row in df_filtered.iterrows():
            ticker = row.get('symbol', row.get('ticker', ''))
            exchange = row.get('comGroupCode', row.get('exchange', row.get('market')))
            industry = row.get('icbName3', row.get('industry', row.get('icb_name3')))
            company_name = row.get('organ_name', row.get('organName', row.get('company_name')))

            if ticker:
                clean_ticker = _clean_text(ticker, fallback="")
                if not clean_ticker:
                    continue
                existing = existing_map.get(clean_ticker, {})
                clean_exchange = (
                    _clean_nullable_text(exchange)
                    or _clean_nullable_text(existing.get("exchange"))
                    or "UNKNOWN"
                )
                clean_industry = _clean_nullable_text(industry) or _clean_nullable_text(existing.get("industry"))
                clean_company_name = _clean_nullable_text(company_name) or _clean_nullable_text(existing.get("company_name"))
                records_to_insert.append({
                    "ticker": clean_ticker,
                    "exchange": clean_exchange,
                    "industry": clean_industry,
                    "company_name": clean_company_name,
                })

        print(f"Found {len(records_to_insert)} tickers.")

        if supabase:
            # Upsert theo batch để tránh payload quá lớn
            batch_size = 100
            upserted_count = 0
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i + batch_size]
                supabase.table("tickers").upsert(batch, on_conflict="ticker").execute()
                upserted_count += len(batch)
            print("Successfully populated the `tickers` table.")
            return {
                "step": "extract_tickers",
                "success": True,
                "records_fetched": len(records_to_insert),
                "records_upserted": upserted_count,
                "errors": 0,
                "error_rate": 0.0,
            }
        else:
            print("Database not configured. Cannot save tickers.")
            return {
                "step": "extract_tickers",
                "success": False,
                "records_fetched": len(records_to_insert),
                "records_upserted": 0,
                "errors": 1,
                "error_rate": 1.0,
                "message": "Database not configured.",
            }

    except Exception as e:
        print(f"Error fetching tickers: {e}")
        import traceback
        traceback.print_exc()
        return {
            "step": "extract_tickers",
            "success": False,
            "records_fetched": 0,
            "records_upserted": 0,
            "errors": 1,
            "error_rate": 1.0,
            "message": str(e),
        }


if __name__ == "__main__":
    fetch_and_store_tickers()
