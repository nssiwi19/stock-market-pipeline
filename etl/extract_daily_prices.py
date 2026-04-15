import time
from datetime import datetime, timedelta
from vnstock import stock_historical_data
from etl import config

def extract_and_upsert_stock_data():
    supabase = config.get_supabase_client()
    
    # 1. Lấy danh sách ticker từ bảng tickers
    response = supabase.table("tickers").select("ticker").execute()
    tickers = [item['ticker'] for item in response.data]
    
    # Cấu hình thời gian lấy dữ liệu (5 ngày gần nhất)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    
    all_records = [] 
    batch_size = 50 
    count = 0

    print(f"🚀 Bắt đầu cào dữ liệu cho {len(tickers)} mã (Batch Size: {batch_size})...")

    for ticker in tickers:
        df = None # FIX LỖI MIM: Luôn khởi tạo df là None ở đầu mỗi vòng lặp
        try:
            # Lấy dữ liệu
            df = stock_historical_data(symbol=ticker, start_date=start_date, end_date=end_date, resolution='1D', type='stock')
            
            # Kiểm tra df tồn tại và không rỗng
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    all_records.append({
                        "ticker": ticker,
                        "trading_date": str(row['time'].date()) if hasattr(row['time'], 'date') else str(row['time']),
                        "open_price": float(row['open']),
                        "high_price": float(row['high']),
                        "low_price": float(row['low']),
                        "close_price": float(row['close']),
                        "volume": int(row['volume'])
                    })
            
            count += 1
            time.sleep(0.1) 

            # FIX LỖI TRÙNG LẶP: Đẩy batch và LÀM TRỐNG danh sách
            if len(all_records) >= batch_size:
                # Thêm tham số on_conflict để Supabase biết đường mà ghi đè
                supabase.table("daily_prices").upsert(
                    all_records, 
                    on_conflict="ticker,trading_date" 
                ).execute()
                
                print(f"✅ Đã Upsert lô {len(all_records)} bản ghi. Tiến độ: {count}/{len(tickers)}")
                all_records = [] # CỰC KỲ QUAN TRỌNG: Phải reset list về rỗng sau khi đẩy thành công

        except Exception as e:
            print(f"⚠️ Lỗi khi xử lý mã {ticker}: {e}")
            # Nếu lỗi, ta bỏ qua mã này và đi tiếp, df = None đã bảo vệ ta khỏi lỗi "referenced before assignment"
            continue 

    # Đừng quên lô cuối cùng còn sót lại
    if all_records:
        supabase.table("daily_prices").upsert(all_records, on_conflict="ticker,trading_date").execute()
        print(f"🏁 Hoàn tất nạp nốt {len(all_records)} bản ghi cuối.")

if __name__ == "__main__":
    extract_and_upsert_stock_data()