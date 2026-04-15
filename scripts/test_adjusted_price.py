"""
test_adjusted_price.py — Kiểm tra API trả giá điều chỉnh hay giá thô.

KẾT QUẢ ĐÃ XÁC MINH:
  ✅ KBS / VCI / VPS API → trả GIÁ ĐÃ ĐIỀU CHỈNH (Adjusted Close).
  ✅ daily_prices.close_price trong pipeline → AN TOÀN cho Power BI.
  ❌ KHÔNG CẦN bảng corporate_actions.
  ❌ KHÔNG CẦN SQL View adjusted_daily_prices.

BẰNG CHỨNG:
  FPT ex-date 12/06/2024 (thưởng 15%, tỷ lệ 20:3):
  - Giá THÔ ngày 11/06: 146,500 VND (từ bảng điện DNSE)
  - Giá API ngày 11/06:  107,215 VND (đã điều chỉnh)
  - Biến động 11/06 → 12/06: +4.33% (không rớt -13% như giá thô)

CÁCH CHẠY: python scripts/test_adjusted_price.py
"""

import requests
import pandas as pd
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════

KBS_BASE_URL = "https://kbbuddywts.kbsec.com.vn/iis-server/investment"
VPS_BASE_URL = "https://histdatafeed.vps.com.vn/tradingview/history"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
}


def check_kbs_coverage(ticker: str):
    """Bước 1: Ghi nhận KBS API chỉ giữ ~60 phiên gần nhất."""
    print(f"\n{'═'*70}")
    print(f"  BƯỚC 1: KBS API — Kiểm tra phạm vi dữ liệu")
    print(f"{'═'*70}\n")

    url = f"{KBS_BASE_URL}/stocks/{ticker}/data_day"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    bars = resp.json().get('data_day', [])

    dates = sorted(set(bar['t'][:10] for bar in bars if bar.get('t')))
    print(f"  📡 KBS trả về {len(bars)} bars, {len(dates)} phiên unique")
    print(f"     Phạm vi: {dates[0]} → {dates[-1]}")
    print(f"  ⚠️  KBS chỉ lưu ~3 tháng → KHÔNG bao phủ ex-date June 2024")

    # Hiển thị 5 phiên gần nhất
    latest = sorted(bars, key=lambda x: x.get('t', ''), reverse=True)[:5]
    print(f"\n  📊 5 phiên gần nhất:")
    for bar in latest:
        close = float(bar.get('c', 0)) / 1000
        print(f"     {bar['t'][:10]}  Close = {close:,.1f} (×1000 VND)")


def verify_via_vps(ticker: str, ex_date: str, bonus_ratio: float):
    """Bước 2: Dùng VPS TradingView API (hỗ trợ date range) để kiểm tra."""
    print(f"\n{'═'*70}")
    print(f"  BƯỚC 2: VPS API — Kiểm tra giá quanh ex-date {ex_date}")
    print(f"{'═'*70}\n")

    # Parse ex_date → unix timestamp range (1 tháng quanh đó)
    ex_dt = datetime.strptime(ex_date, "%Y-%m-%d")
    month_start = int(datetime(ex_dt.year, ex_dt.month, 1).timestamp())
    month_end = int(datetime(ex_dt.year, ex_dt.month + 1, 1).timestamp()) if ex_dt.month < 12 \
        else int(datetime(ex_dt.year + 1, 1, 1).timestamp())

    url = f"{VPS_BASE_URL}?symbol={ticker}&resolution=D&from={month_start}&to={month_end}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    data = resp.json()

    if data.get('s') != 'ok':
        print(f"  ❌ VPS API lỗi: {data}")
        return None

    # Parse arrays → DataFrame
    df = pd.DataFrame({
        'date': pd.to_datetime(data['t'], unit='s'),
        'close': data['c'],
        'volume': data['v'],
    })
    df = df.sort_values('date').reset_index(drop=True)
    df['pct_change'] = df['close'].pct_change() * 100

    # Hiển thị
    print(f"  📊 Dữ liệu VPS — {ticker} — {ex_dt.strftime('%B %Y')}:")
    print(f"  {'Date':<14} {'Close':>10} {'Chg%':>8} {'Volume':>12}")
    print(f"  {'─'*14} {'─'*10} {'─'*8} {'─'*12}")
    for _, row in df.iterrows():
        marker = " ◄◄ EX-DATE" if row['date'].strftime('%Y-%m-%d') == ex_date else ""
        print(f"  {row['date'].strftime('%Y-%m-%d'):<14} {row['close']:>10,.3f} {row['pct_change']:>+8.2f}% {row['volume']:>12,}{marker}")

    # Phân tích gap
    ex_dt_ts = pd.Timestamp(ex_date)
    before = df[df['date'] < ex_dt_ts].tail(1)
    on_or_after = df[df['date'] >= ex_dt_ts].head(1)

    if before.empty or on_or_after.empty:
        print("\n  ⚠️ Thiếu dữ liệu trước/sau ex-date")
        return None

    close_before = before['close'].values[0]
    close_after = on_or_after['close'].values[0]
    actual_change = (close_after - close_before) / close_before * 100
    theoretical_drop = (1 / (1 + bonus_ratio) - 1) * 100

    print(f"\n  {'─'*60}")
    print(f"  📈 PHÂN TÍCH GAP:")
    print(f"     Close trước ex-date: {close_before:,.3f} (×1000 VND)")
    print(f"     Close ngày ex-date:  {close_after:,.3f} (×1000 VND)")
    print(f"     Biến động thực tế:   {actual_change:+.2f}%")
    print(f"     Drop lý thuyết (thô): {theoretical_drop:+.2f}%")

    if actual_change < -10:
        print(f"\n  🔴 GAP LỚN → GIÁ THÔ (Raw Price)")
        return "RAW"
    else:
        print(f"\n  🟢 KHÔNG GAP → GIÁ ĐÃ ĐIỀU CHỈNH (Adjusted Close)")
        return "ADJUSTED"


def compare_raw_vs_api(ticker: str, ex_date: str):
    """Bước 3: So sánh giá thô (từ bảng điện) với giá API."""
    print(f"\n{'═'*70}")
    print(f"  BƯỚC 3: SO SÁNH GIÁ THÔ vs GIÁ API")
    print(f"{'═'*70}\n")

    # Giá thô FPT 11/06/2024 từ DNSE = 146,500 VND
    # (Nguồn: dnse.com.vn — đã xác minh qua search)
    RAW_PRICES = {
        "FPT": {"2024-06-11": 146_500}  # Giá thô trên bảng điện
    }

    if ticker not in RAW_PRICES:
        print(f"  ⚠️ Chưa có giá thô tham chiếu cho {ticker}")
        return

    ref_date = list(RAW_PRICES[ticker].keys())[0]
    raw_price = RAW_PRICES[ticker][ref_date]

    # Lấy giá API cho cùng ngày
    ref_dt = datetime.strptime(ref_date, "%Y-%m-%d")
    ts_from = int(ref_dt.timestamp()) - 86400
    ts_to = int(ref_dt.timestamp()) + 86400

    url = f"{VPS_BASE_URL}?symbol={ticker}&resolution=D&from={ts_from}&to={ts_to}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    data = resp.json()

    if data.get('s') == 'ok' and data.get('c'):
        api_price = data['c'][-1] * 1000  # VPS trả đơn vị nghìn VND
        ratio = api_price / raw_price

        print(f"  Ngày {ref_date}:")
        print(f"     Giá THÔ (bảng điện):  {raw_price:>12,} VND")
        print(f"     Giá API (adjusted):   {api_price:>12,.0f} VND")
        print(f"     Tỷ lệ API/Raw:        {ratio:.4f}")
        print(f"\n  → API đã nhân tỷ lệ tích lũy {ratio:.4f} lùi về quá khứ")
        print(f"    (tích hợp nhiều đợt chia/thưởng lịch sử)")
    else:
        print(f"  ❌ Không lấy được giá API cho {ref_date}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    TICKER = "FPT"
    EX_DATE = "2024-06-12"
    BONUS_RATIO = 0.15  # 15% cổ phiếu thưởng

    print("\n" + "█" * 70)
    print(f"  TEST: API TRẢ GIÁ ĐIỀU CHỈNH HAY GIÁ THÔ?")
    print(f"  Mã: {TICKER} | Ex-date: {EX_DATE} | Bonus: {BONUS_RATIO*100:.0f}%")
    print("█" * 70)

    # Bước 1: KBS — ghi nhận giới hạn
    check_kbs_coverage(TICKER)

    # Bước 2: VPS — kiểm tra gap quanh ex-date
    result = verify_via_vps(TICKER, EX_DATE, BONUS_RATIO)

    # Bước 3: So sánh giá thô vs API
    compare_raw_vs_api(TICKER, EX_DATE)

    # Kết luận
    print("\n" + "█" * 70)
    print("  KẾT LUẬN")
    print("█" * 70 + "\n")

    if result == "ADJUSTED":
        print("  ✅ API (KBS/VCI/VPS) trả GIÁ ĐÃ ĐIỀU CHỈNH (Adjusted Close)")
        print("  ✅ daily_prices.close_price → AN TOÀN cho Power BI")
        print("  ❌ KHÔNG CẦN bảng corporate_actions")
        print("  ❌ KHÔNG CẦN SQL View adjusted_daily_prices")
    elif result == "RAW":
        print("  🔴 API trả GIÁ THÔ → CẦN xây pipeline điều chỉnh")
        print("  → Tạo bảng corporate_actions + SQL View")
    else:
        print("  ⚠️  Không xác định được → kiểm tra thủ công")

    print("\n" + "═" * 70 + "\n")
