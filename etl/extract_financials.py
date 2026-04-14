import pandas as pd
from vnstock import financial_report
from .config import get_supabase_client
import time

def extract_metric(df, keywords, period_col):
    """
    Hàm tìm kiếm an toàn một chỉ tiêu tài chính dựa trên từ khóa.
    Xử lý cả doanh nghiệp thường và ngân hàng.
    """
    if df is None or df.empty:
        return None
        
    metric_col = df.columns[0] # Cột đầu tiên thường chứa tên chỉ tiêu
    for kw in keywords:
        # Tìm dòng có chứa từ khóa
        row = df[df[metric_col].astype(str).str.contains(kw, case=False, na=False)]
        if not row.empty:
            val = row.iloc[0][period_col]
            try:
                # Ép kiểu float, vnstock thường trả về số nguyên (VND) hoặc Tỷ VND
                # Đưa tất cả về đơn vị Tỷ VNĐ cho dễ nhìn trên Dashboard
                num = float(val)
                return num / 1e9 if num > 1e6 else num 
            except:
                return None
    return None

def fetch_and_store_financials():
    supabase = get_supabase_client()
    if not supabase:
        print("❌ Database not configured. Exiting.")
        return False

    print("📡 Đang lấy danh sách mã từ database...")
    try:
        response = supabase.table("tickers").select("ticker").execute()
        tickers = [item['ticker'] for item in response.data]
    except Exception as e:
        print(f"🛑 Lỗi truy vấn danh sách mã: {e}")
        return False

    print(f"🚀 Bắt đầu cào Báo cáo tài chính cho {len(tickers)} mã...")

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Đang xử lý BCTC: {ticker}...")
        try:
            # 1. Tải dữ liệu BCTC (Kỳ: Quý)
            df_is = financial_report(symbol=ticker, report_type='IncomeStatement', frequency='Quarterly')
            df_bs = financial_report(symbol=ticker, report_type='BalanceSheet', frequency='Quarterly')

            if df_is is None or df_bs is None or df_is.empty or df_bs.empty:
                print(f"  ⚠️ Bỏ qua {ticker} vì thiếu dữ liệu BCTC.")
                continue

            # Lấy danh sách các Quý (Các cột có chứa chữ 'Q')
            periods = [col for col in df_is.columns if 'Q' in str(col).upper()]
            records = []

            for period in periods:
                # Chuẩn hóa tên kỳ: 'Q1 2024' -> 'Q1-2024'
                period_str = str(period).replace(' ', '-').upper()

                # 2. Trích xuất các chỉ tiêu cốt lõi bằng từ khóa
                # Doanh thu: Ưu tiên 'Doanh thu thuần', với Bank là 'Thu nhập lãi thuần'
                revenue = extract_metric(df_is, ['Doanh thu thuần', 'Thu nhập lãi thuần'], period)
                
                # Lợi nhuận sau thuế
                profit = extract_metric(df_is, ['Lợi nhuận sau thuế'], period)
                
                # Bảng cân đối kế toán
                assets = extract_metric(df_bs, ['Tổng cộng tài sản', 'Tổng tài sản'], period)
                liabilities = extract_metric(df_bs, ['Nợ phải trả'], period)
                equity = extract_metric(df_bs, ['Vốn chủ sở hữu'], period)

                record = {
                    "ticker": ticker,
                    "report_type": "quarterly",
                    "period": period_str,
                    "revenue": revenue,
                    "profit_after_tax": profit,
                    "total_assets": assets,
                    "total_liabilities": liabilities,
                    "equity": equity,
                    # EPS có thể tính nhẩm: (LNST * 4) / (Equity / Mệnh giá) hoặc lấy từ financial_ratio
                    # Tạm thời để Null nếu không có sẵn trực tiếp trong BCTC
                    "eps": None 
                }
                
                # Chỉ lưu nếu có dữ liệu cơ bản
                if revenue is not None or assets is not None:
                    records.append(record)

            # 3. Đẩy lên Supabase (Upsert)
            if records:
                supabase.table("financial_reports").upsert(records).execute()
                print(f"  ✅ Đã nạp BCTC {len(records)} quý cho {ticker}.")

            time.sleep(1) # Nghỉ 1 giây để tránh Rate Limit của API

        except Exception as e:
            print(f"  ❌ Lỗi tại mã {ticker}: {e}")
            
    return True

if __name__ == "__main__":
    fetch_and_store_financials()
