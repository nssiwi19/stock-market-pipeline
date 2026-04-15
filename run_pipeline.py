"""
Pipeline Runner - Thống nhất luồng ETL chứng khoán

Extract Tickers -> Extract Daily Prices
"""

import sys
import os
import traceback
from datetime import datetime, timezone, timedelta
from etl import extract_financials
from etl import extract_tickers
from etl import extract_daily_prices
from etl import config
from etl import notifier
from etl.ai_agent import get_ai_market_summary
import pandas as pd


def run_step(step_name, func, *args, **kwargs):
    """Chạy một bước của pipeline và bắt lỗi."""
    print(f"\n{'='*60}")
    print(f"🔄 ĐANG CHẠY: {step_name}")
    print(f"{'='*60}\n")

    vn_tz = timezone(timedelta(hours=7))
    start = datetime.now(vn_tz)

    try:
        # Chạy hàm thực thi của module
        result = func(*args, **kwargs)

        duration = (datetime.now(vn_tz) - start).total_seconds()
        print(f"\n✅ {step_name} hoàn tất ({duration:.1f}s)")
        return True, result

    except Exception as e:
        duration = (datetime.now(vn_tz) - start).total_seconds()
        print(f"\n❌ {step_name} THẤT BẠI sau {duration:.1f}s")
        print(f"   Lỗi: {e}")
        traceback.print_exc()
        return False, None

def main():
    vn_tz = timezone(timedelta(hours=7))
    start_time = datetime.now(vn_tz)

    print("╔" + "═"*58 + "╗")
    print("║   📈  STOCK MARKET PIPELINE — Automated Run            ║")
    print("╚" + "═"*58 + "╝")
    print(f"🕐 Bắt đầu: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🖥️  Máy chủ: {os.environ.get('GITHUB_ACTIONS', 'Local')}")

    # Kiểm tra config
    try:
        client = config.get_supabase_client()
        print("✅ Đã kết nối Supabase thành công.")
    except Exception as e:
        print(f"🛑 Lỗi kết nối Supabase: {e}")
        sys.exit(1)

    results = {}

    # Bước 1: Extract Tickers
    success, _ = run_step(
        "Bước 1: EXTRACT TICKERS — Lấy danh sách mã chứng khoán",
        extract_tickers.fetch_and_store_tickers
    )
    results["Extract Tickers"] = success

    if not success:
        error_msg = "❌ Pipeline dừng lại: Extract Tickers thất bại."
        print(f"\n{error_msg}")
        notifier.send_telegram_msg(f"🚨 *Stock Pipeline Alert*\n{error_msg}")
        sys.exit(1)

    # Bước 2: Extract Daily Prices
    success, _ = run_step(
        "Bước 2: EXTRACT DAILY PRICES — Lấy lịch sử giá hàng ngày",
        extract_daily_prices.extract_and_upsert_stock_data
    )
    results["Extract Daily Prices"] = success

    if not success:
        error_msg = "❌ Pipeline dừng lại: Extract Daily Prices thất bại."
        print(f"\n{error_msg}")
        notifier.send_telegram_msg(f"🚨 *Stock Pipeline Alert*\n{error_msg}")
        sys.exit(1)

    # Bước 3: Extract Financials
    success, _ = run_step(
        "Bước 3: EXTRACT FINANCIALS — Lấy báo cáo tài chính",
        extract_financials.fetch_and_store_financials
    )
    results["Extract Financials"] = success

    if not success:
        # Nếu lấy BCTC lỗi, chỉ báo cho user biết chứ không dừng pipeline để vẫn xuất được báo cáo giá
        warn_msg = "⚠️ Cảnh báo: Lấy dữ liệu tài chính (Extract Financials) thất bại. Kiểm tra log."
        print(f"\n{warn_msg}")
        notifier.send_telegram_msg(f"🚨 *Stock Pipeline Warning*\n{warn_msg}")

    # Tổng kết
    total_duration = (datetime.now(vn_tz) - start_time).total_seconds()

    print("\n" + "╔" + "═"*58 + "╗")
    print("║   📊  BÁO CÁO PIPELINE                                 ║")
    print("╚" + "═"*58 + "╝")

    for step, is_success in results.items():
        icon = "✅" if is_success else "❌"
        print(f"   {icon} {step}")

    print(f"\n   ⏱️  Tổng thời gian: {total_duration:.1f}s")
    print(f"   🕐 Kết thúc: {datetime.now(vn_tz).strftime('%Y-%m-%d %H:%M:%S')}")

    if all(results.values()):
        print("\n   🎉 PIPELINE CHẠY THÀNH CÔNG!")
        
        try:
            # Lấy dữ liệu Top 5 thanh khoản hôm nay từ Supabase
            client = config.get_supabase_client()
            today_str = datetime.now(vn_tz).strftime('%Y-%m-%d')
            
            response = client.table('daily_prices')\
                .select('ticker, close_price, volume')\
                .eq('trading_date', today_str)\
                .order('volume', desc=True)\
                .limit(5).execute()

            if response.data and len(response.data) > 0:
                df_top = pd.DataFrame(response.data)
                
                # --- KÍCH HOẠT AI AGENT TẠI ĐÂY ---
                ai_insight = get_ai_market_summary(df_top)
                
                # --- GHÉP CHỮ VÀO TIN NHẮN ---
                msg = f"✅ *BÁO CÁO THỊ TRƯỜNG {datetime.now(vn_tz).strftime('%d/%m/%Y')}*\n"
                msg += f"⏱️ Thời gian Pipeline cào dữ liệu: {total_duration:.1f}s\n\n"
                msg += f"{ai_insight}\n" # Chèn đoạn văn của AI vào đây
                
                # Gửi báo cáo kèm biểu đồ
                notifier.send_telegram_report_with_chart(df_top, msg)
            else:
                # Nếu không có dữ liệu hôm nay (ví dụ chưa cào xong hoặc ngày lễ)
                status_msg = f"🎉 *PIPELINE CHẠY THÀNH CÔNG!*\n- Thời gian: {total_duration:.1f}s\n- Lưu ý: Không có dữ liệu giao dịch mới để vẽ biểu đồ."
                notifier.send_telegram_msg(status_msg)
                
        except Exception as e:
            print(f"❌ Lỗi khi tạo báo cáo biểu đồ: {e}")
            notifier.send_telegram_msg(f"✅ *PIPELINE THÀNH CÔNG*\n⏱️ Tổng thời gian: {total_duration:.1f}s\n(Lỗi tạo biểu đồ: {e})")
    else:

        print("\n   ⚠️  Pipeline có lỗi. Kiểm tra log ở trên.")
        notifier.send_telegram_msg("⚠️ *Stock Pipeline Warning*\nPipeline kết thúc với một số lỗi. Kiểm tra log.")
        sys.exit(1)

if __name__ == "__main__":
    main()
