"""
dqc_missing_dates.py — Data Quality Check: Kiểm tra tính toàn vẹn thời gian.

NGUYÊN TẮC:
  Cổ phiếu giao dịch từ Thứ 2 → Thứ 6 (Business Days).
  Script này đối chiếu dữ liệu trong daily_prices với lịch giao dịch chuẩn
  để phát hiện những ngày bị "thủng lỗ" (missing trading dates).

XỬ LÝ NGÀY NGHỈ:
  - Loại bỏ T7/CN tự động (freq='B')
  - Loại bỏ ngày lễ quốc gia Việt Nam (Tết Âm lịch, 30/4, 1/5, 2/9, Giỗ Tổ...)
  - Loại bỏ ngày nghỉ bù theo lịch HOSE (nếu biết)

OUTPUT:
  - Báo cáo tổng hợp: tổng số mã bị thiếu, top offenders
  - Chi tiết từng mã: ngày nào bị thiếu, phân chia theo tháng

CÁCH CHẠY:
  python scripts/dqc_missing_dates.py                     # Quét toàn bộ
  python scripts/dqc_missing_dates.py --ticker FPT         # Quét 1 mã
  python scripts/dqc_missing_dates.py --top 20             # Top 20 mã thiếu nhiều nhất
  python scripts/dqc_missing_dates.py --ticker FPT --detail # Chi tiết từng ngày
"""

import sys
import os
import argparse
from datetime import date

import pandas as pd

# ── Thêm project root vào path ──
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl import config


# ═══════════════════════════════════════════════════════════════════
# LỊCH NGÀY NGHỈ LỄ VIỆT NAM (HOSE đóng cửa)
# ═══════════════════════════════════════════════════════════════════
# Ghi chú: Tết Âm lịch thay đổi hàng năm theo lịch Mặt Trăng.
# Danh sách dưới đây bao gồm CẢ ngày nghỉ bù chính thức.
# Cập nhật hàng năm khi HOSE công bố lịch nghỉ.

VN_HOLIDAYS = [
    # ── 2024 ──
    # Tết Dương lịch
    date(2024, 1, 1),
    # Tết Âm lịch 2024 (Giáp Thìn) — HOSE nghỉ 08/02 → 14/02
    date(2024, 2, 8), date(2024, 2, 9),
    date(2024, 2, 12), date(2024, 2, 13), date(2024, 2, 14),
    # Giỗ Tổ Hùng Vương (10/3 AL = 18/04/2024)
    date(2024, 4, 18),
    # 30/4 + 1/5
    date(2024, 4, 30), date(2024, 5, 1),
    # Quốc khánh 2/9 + nghỉ bù
    date(2024, 9, 2), date(2024, 9, 3),

    # ── 2025 ──
    # Tết Dương lịch
    date(2025, 1, 1),
    # Tết Âm lịch 2025 (Ất Tỵ) — HOSE nghỉ 25/01 → 02/02 (ước tính)
    date(2025, 1, 27), date(2025, 1, 28), date(2025, 1, 29),
    date(2025, 1, 30), date(2025, 1, 31),
    # Giỗ Tổ Hùng Vương (10/3 AL = 07/04/2025)
    date(2025, 4, 7),
    # 30/4 + 1/5
    date(2025, 4, 30), date(2025, 5, 1), date(2025, 5, 2),
    # Quốc khánh 2/9
    date(2025, 9, 1), date(2025, 9, 2),

    # ── 2026 ──
    # Tết Dương lịch
    date(2026, 1, 1), date(2026, 1, 2),
    # Tết Âm lịch 2026 (Bính Ngọ) — HOSE nghỉ 16/02 → 22/02 (ước tính)
    date(2026, 2, 16), date(2026, 2, 17), date(2026, 2, 18),
    date(2026, 2, 19), date(2026, 2, 20),
    # Giỗ Tổ Hùng Vương (10/3 AL ≈ 26/04/2026)
    date(2026, 4, 26),
    # 30/4 + 1/5
    date(2026, 4, 30), date(2026, 5, 1),
    # Quốc khánh 2/9
    date(2026, 9, 2),
]

# Convert sang set of Timestamps để lookup O(1)
VN_HOLIDAYS_SET = set(pd.Timestamp(d) for d in VN_HOLIDAYS)


# ═══════════════════════════════════════════════════════════════════
# CORE LOGIC
# ═══════════════════════════════════════════════════════════════════

def build_expected_trading_dates(min_date: pd.Timestamp, max_date: pd.Timestamp) -> pd.DatetimeIndex:
    """
    Tạo dải ngày giao dịch chuẩn: Business Days - Ngày lễ VN.

    Args:
        min_date: Ngày bắt đầu (inclusive)
        max_date: Ngày kết thúc (inclusive)

    Returns:
        DatetimeIndex chỉ chứa ngày giao dịch hợp lệ
    """
    # freq='B' = Business days (T2-T6), tự loại T7/CN
    all_business_days = pd.date_range(start=min_date, end=max_date, freq='B')

    # Loại bỏ ngày lễ VN
    trading_days = all_business_days[~all_business_days.isin(VN_HOLIDAYS_SET)]

    return trading_days


def fetch_ticker_dates(supabase, ticker: str) -> pd.DataFrame:
    """
    Kéo toàn bộ trading_date của 1 mã từ Supabase.

    Supabase REST API limit mặc định = 1000 rows.
    Dùng pagination để lấy đủ nếu mã có >1000 phiên.
    """
    all_data = []
    page_size = 1000
    offset = 0

    while True:
        res = (
            supabase.table("daily_prices")
            .select("trading_date")
            .eq("ticker", ticker)
            .order("trading_date")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = res.data
        if not batch:
            break
        all_data.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    return pd.DataFrame(all_data)


def check_missing_trading_days(ticker: str, supabase=None, verbose: bool = False) -> dict:
    """
    Kiểm tra 1 mã cổ phiếu có bị thủng lỗ ngày giao dịch không.

    Args:
        ticker:   Mã cổ phiếu (VD: "FPT")
        supabase: Supabase client (nếu None sẽ tự khởi tạo)
        verbose:  In chi tiết từng ngày thiếu

    Returns:
        dict: {
            'ticker': str,
            'total_actual': int,      # Tổng phiên thực tế trong DB
            'total_expected': int,    # Tổng phiên kỳ vọng (B-days - lễ)
            'total_missing': int,     # Số phiên bị thiếu
            'missing_dates': list,    # Danh sách ngày thiếu
            'coverage_pct': float,    # % bao phủ
            'date_range': tuple,      # (min_date, max_date)
        }
    """
    if supabase is None:
        supabase = config.get_supabase_client()

    df = fetch_ticker_dates(supabase, ticker)

    if df.empty:
        return {
            'ticker': ticker,
            'total_actual': 0,
            'total_expected': 0,
            'total_missing': 0,
            'missing_dates': [],
            'coverage_pct': 0.0,
            'date_range': (None, None),
        }

    df['trading_date'] = pd.to_datetime(df['trading_date'])
    df = df.sort_values('trading_date').drop_duplicates(subset='trading_date')

    min_date = df['trading_date'].min()
    max_date = df['trading_date'].max()

    # Dải ngày giao dịch chuẩn (B-days trừ lễ)
    expected_dates = build_expected_trading_dates(min_date, max_date)

    # Đối chiếu
    actual_dates = set(df['trading_date'])
    missing_dates = sorted([d for d in expected_dates if d not in actual_dates])

    total_expected = len(expected_dates)
    total_actual = len(actual_dates)
    total_missing = len(missing_dates)
    coverage_pct = (total_actual / total_expected * 100) if total_expected > 0 else 0.0

    # In kết quả
    if total_missing == 0:
        print(f"  ✅ {ticker:<8} | {total_actual:>5} phiên | {min_date.date()} → {max_date.date()} | 100% bao phủ")
    else:
        print(f"  ⚠️  {ticker:<8} | {total_actual:>5}/{total_expected} phiên | "
              f"THIẾU {total_missing:>3} ngày | {coverage_pct:.1f}% bao phủ")

        if verbose:
            # Nhóm ngày thiếu theo tháng
            missing_df = pd.DataFrame({'date': missing_dates})
            missing_df['month'] = missing_df['date'].dt.to_period('M')
            grouped = missing_df.groupby('month')['date'].apply(list)

            for month, dates in grouped.items():
                date_strs = [d.strftime('%d') for d in dates]
                print(f"     └─ {month}: ngày {', '.join(date_strs)}")

    return {
        'ticker': ticker,
        'total_actual': total_actual,
        'total_expected': total_expected,
        'total_missing': total_missing,
        'missing_dates': missing_dates,
        'coverage_pct': coverage_pct,
        'date_range': (min_date.date(), max_date.date()),
    }


def run_full_scan(top_n: int = 10, verbose: bool = False):
    """
    Quét toàn bộ mã trong bảng tickers.

    Args:
        top_n:   Hiển thị top N mã thiếu nhiều nhất
        verbose: In chi tiết ngày thiếu cho từng mã
    """
    supabase = config.get_supabase_client()

    # Lấy danh sách tickers
    res = supabase.table("tickers").select("ticker").execute()
    tickers = sorted([item['ticker'] for item in res.data])

    print(f"\n{'█'*70}")
    print(f"  DATA QUALITY CHECK: MISSING TRADING DATES")
    print(f"  Tổng số mã: {len(tickers)}")
    print(f"  Ngày lễ VN đã loại trừ: {len(VN_HOLIDAYS)} ngày")
    print(f"{'█'*70}\n")

    results = []
    clean_count = 0
    empty_count = 0

    for i, ticker in enumerate(tickers):
        result = check_missing_trading_days(ticker, supabase=supabase, verbose=verbose)
        results.append(result)

        if result['total_actual'] == 0:
            empty_count += 1
        elif result['total_missing'] == 0:
            clean_count += 1

    # ── Báo cáo tổng hợp ──
    results_with_data = [r for r in results if r['total_actual'] > 0]
    results_missing = [r for r in results_with_data if r['total_missing'] > 0]

    print(f"\n{'═'*70}")
    print(f"  TỔNG KẾT")
    print(f"{'═'*70}\n")

    print(f"  📊 Tổng mã quét:           {len(tickers)}")
    print(f"  ✅ Mã đầy đủ (100%):       {clean_count}")
    print(f"  ⚠️  Mã bị thiếu ngày:       {len(results_missing)}")
    print(f"  🚫 Mã không có dữ liệu:    {empty_count}")

    if results_missing:
        # Sắp xếp theo số ngày thiếu giảm dần
        top_offenders = sorted(results_missing, key=lambda x: x['total_missing'], reverse=True)[:top_n]

        print(f"\n  {'─'*60}")
        print(f"  🏆 TOP {min(top_n, len(top_offenders))} MÃ THIẾU NHIỀU NHẤT:")
        print(f"  {'─'*60}")
        print(f"  {'Mã':<10} {'Thiếu':>6} {'Tổng':>6} {'Bao phủ':>8} {'Phạm vi'}")
        print(f"  {'─'*10} {'─'*6} {'─'*6} {'─'*8} {'─'*24}")

        for r in top_offenders:
            start, end = r['date_range']
            print(f"  {r['ticker']:<10} {r['total_missing']:>6} {r['total_expected']:>6} "
                  f"{r['coverage_pct']:>7.1f}% {start} → {end}")

    # Tổng missing trên toàn bộ
    total_missing_all = sum(r['total_missing'] for r in results_with_data)
    total_expected_all = sum(r['total_expected'] for r in results_with_data)
    global_coverage = (1 - total_missing_all / total_expected_all) * 100 if total_expected_all > 0 else 0

    print(f"\n  📈 Coverage toàn pipeline: {global_coverage:.2f}%")
    print(f"     ({total_expected_all - total_missing_all:,}/{total_expected_all:,} data points)")
    print(f"\n{'═'*70}\n")

    return results


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="DQC: Kiểm tra tính toàn vẹn thời gian trong daily_prices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python scripts/dqc_missing_dates.py                      # Quét toàn bộ
  python scripts/dqc_missing_dates.py --ticker FPT          # Quét 1 mã
  python scripts/dqc_missing_dates.py --ticker FPT --detail  # Chi tiết từng ngày
  python scripts/dqc_missing_dates.py --top 20              # Top 20 mã thiếu nhiều nhất
        """
    )
    parser.add_argument('--ticker', type=str, help='Kiểm tra 1 mã cụ thể')
    parser.add_argument('--top', type=int, default=10, help='Số lượng top offenders (mặc định: 10)')
    parser.add_argument('--detail', action='store_true', help='In chi tiết ngày thiếu theo tháng')

    args = parser.parse_args()

    if args.ticker:
        print(f"\n{'█'*70}")
        print(f"  DQC: KIỂM TRA MÃ {args.ticker.upper()}")
        print(f"{'█'*70}\n")
        result = check_missing_trading_days(
            args.ticker.upper(),
            verbose=args.detail
        )

        if result['total_missing'] > 0 and not args.detail:
            print(f"\n  💡 Thêm --detail để xem chi tiết ngày thiếu theo tháng.")

        print()
    else:
        run_full_scan(top_n=args.top, verbose=args.detail)


if __name__ == "__main__":
    main()
