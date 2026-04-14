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
        extract_daily_prices.fetch_and_store_daily_prices
    )
    results["Extract Daily Prices"] = success

    if not success:
        error_msg = "❌ Pipeline dừng lại: Extract Daily Prices thất bại."
        print(f"\n{error_msg}")
        notifier.send_telegram_msg(f"🚨 *Stock Pipeline Alert*\n{error_msg}")
        sys.exit(1)

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
        status_msg = "🎉 *PIPELINE CHẠY THÀNH CÔNG!*\n"
        status_msg += f"- Thời gian: {total_duration:.1f}s\n"
        status_msg += "- Tickers: ✅\n"
        status_msg += "- Prices: ✅"
        
        print("\n   🎉 PIPELINE CHẠY THÀNH CÔNG!")
        notifier.send_telegram_msg(f"📈 *Stock Pipeline Status*\n{status_msg}")
    else:
        print("\n   ⚠️  Pipeline có lỗi. Kiểm tra log ở trên.")
        notifier.send_telegram_msg("⚠️ *Stock Pipeline Warning*\nPipeline kết thúc với một số lỗi. Kiểm tra log.")
        sys.exit(1)

if __name__ == "__main__":
    main()
