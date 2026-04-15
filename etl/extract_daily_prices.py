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
    
    all_records = [] # Danh sách tạm để gom hàng
    batch_size = 50  # Cứ 50 mã thì đẩy lên Supabase 1 lần
    count = 0

    print(f"🚀 Bắt đầu cào dữ liệu cho {len(tickers)} mã (Batch Size: {batch_size})...")

    for ticker in tickers:
        try:
            # Lấy dữ liệu từ vnstock
            df = stock_historical_data(symbol=ticker, start_date=start_date, end_date=end_date, resolution='1D', type='stock')
            
            if not df.empty:
                # Chuyển DataFrame thành danh sách các dictionary chuẩn schema
                for _, row in df.iterrows():
                    record = {
                        "ticker": ticker,
                        "trading_date": row['time'].strftime('%Y-%m-%d') if hasattr(row['time'], 'strftime') else str(row['time']),
                        "open_price": float(row['open']),
                        "high_price": float(row['high']),
                        "low_price": float(row['low']),
                        "close_price": float(row['close']),
                        "volume": int(row['volume'])
                    }
                    all_records.append(record)
            
            count += 1
            # Giảm thời gian nghỉ xuống còn 0.1s vì ta không gọi DB liên tục nữa
            time.sleep(0.1) 

            # 2. KIỂM TRA VÀ ĐẨY BATCH
            if len(all_records) >= batch_size:
                supabase.table("daily_prices").upsert(all_records).execute()
                print(f"✅ Đã Upsert thành công lô {batch_size} mã. (Tiến độ: {count}/{len(tickers)})")
                all_records = [] # Reset lại danh sách tạm

        except Exception as e:
            print(f"⚠️ Lỗi khi lấy mã {ticker}: {e}")
            continue

    # 3. ĐẨY NỐT NHỮNG MÃ CÒN DƯ (nếu có)
    if all_records:
        supabase.table("daily_prices").upsert(all_records).execute()
        print(f"🏁 Đã hoàn tất đẩy nốt {len(all_records)} bản ghi cuối cùng.")