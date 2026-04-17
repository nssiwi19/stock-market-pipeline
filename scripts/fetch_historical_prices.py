import os
from vnstock import stock_historical_data, listing_companies
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import time
import datetime


def _get_db_uri() -> str:
    """Lấy DB URI từ biến môi trường để tránh lộ thông tin nhạy cảm."""
    db_uri = os.getenv("SUPABASE_DB_URI") or os.getenv("DATABASE_URL")
    if not db_uri:
        raise ValueError("Thiếu SUPABASE_DB_URI (hoặc DATABASE_URL) trong biến môi trường.")
    return db_uri


engine = create_engine(_get_db_uri())

def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_stmt = insert(table.table).values(data)
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['ticker', 'trading_date'],
        set_={c.name: c for c in insert_stmt.excluded if c.name not in ['ticker', 'trading_date']}
    )
    conn.execute(update_stmt)

def crawl_all_history():
    print("📡 Đang lấy danh sách mã...")
    try:
        tickers = listing_companies()['ticker'].tolist()
    except Exception as e:
        print(f"❌ Lỗi khi lấy danh sách mã: {e}")
        return
        
    total = len(tickers)
    
    # Bạn có thể đổi end_date thành ngày hiện tại tịnh tiến được
    end_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    print(f"🚀 Bắt đầu cào lịch sử cho {total} mã (từ 2025-01-01 đến {end_date_str})...")
    
    for i, ticker in enumerate(tickers):
        try:
            # Lấy dữ liệu từ đầu 2025 đến nay
            df = stock_historical_data(symbol=ticker, 
                                       start_date='2025-01-01', 
                                       end_date=end_date_str, 
                                       resolution='1D', type='stock')
            
            if df is not None and not df.empty:
                # --- PHẦN TRANSFORM & LOAD ---
                df = df.copy()
                
                # Đổi tên cột cho khớp với database
                df.rename(columns={
                    'time': 'trading_date',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price',
                    'close': 'close_price'
                }, inplace=True)
                
                # vnstock có thể trả về cột 'ticker', nhưng ta gán đè cho chắc chắn
                df['ticker'] = ticker
                
                # Đảm bảo định dạng chuẩn của Ngày giao dịch (YYYY-MM-DD)
                df['trading_date'] = pd.to_datetime(df['trading_date']).dt.strftime('%Y-%m-%d')
                
                # Chọn đúng các cột quan trọng theo Schema của bảng daily_prices
                cols_to_keep = ['ticker', 'trading_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
                
                if set(cols_to_keep).issubset(df.columns):
                    df_load = df[cols_to_keep]
                    
                    # Batch Upsert qua SQLAlchemy
                    df_load.to_sql('daily_prices', engine, if_exists='append', index=False, method=postgres_upsert)
                    print(f"[{i+1}/{total}] ✅ {ticker}: Đã nạp thành công {len(df_load)} dòng.")
                else:
                    print(f"[{i+1}/{total}] ⚠️ {ticker}: Bỏ qua vì thiếu dữ liệu cột ({df.columns.tolist()}).")
            else:
                print(f"[{i+1}/{total}] ⚠️ {ticker}: Không có dữ liệu giao dịch trong quãng thời gian này.")
                
            # Nghỉ 0.5 giây để tránh bị API chặn (Rate limit)
            time.sleep(0.5) 
            
        except Exception as e:
            print(f"❌ Lỗi tại mã {ticker}: {e}")
            continue

if __name__ == "__main__":
    crawl_all_history()
