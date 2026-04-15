import time
import cloudscraper
import requests

# Bẻ khóa Cloudflare bằng cách ghi đè hàm requests mặc định
scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False})
requests.get = scraper.get
requests.post = scraper.post

from vnstock import financial_report
from etl import config

def extract_metric(df, keywords, period_col):
    """Trích xuất chỉ tiêu tài chính an toàn"""
    if df is None or df.empty:
        return None
        
    metric_col = df.columns[0]
    for kw in keywords:
        row = df[df[metric_col].astype(str).str.contains(kw, case=False, na=False)]
        if not row.empty:
            val = row.iloc[0][period_col]
            try:
                num = float(val)
                # Chuyển về tỷ VND
                return num / 1e9 if num > 1e6 else num 
            except:
                return None
    return None

def fetch_and_store_financials():
    supabase = config.get_supabase_client()
    
    response = supabase.table("tickers").select("ticker").execute()
    tickers = [item['ticker'] for item in response.data]
    
    all_financials = []
    batch_size = 20 # Báo cáo tài chính nặng hơn nên ta để batch nhỏ hơn giá
    count = 0

    print(f"📊 Bắt đầu cào Báo cáo tài chính cho {len(tickers)} mã...")

    for ticker in tickers:
        try:
            # DÙNG financial_report NHƯ LÚC ĐẦU MỚI ĐÚNG VỚI VNSTOCK
            df_is = financial_report(symbol=ticker, report_type='IncomeStatement', frequency='Quarterly')
            df_bs = financial_report(symbol=ticker, report_type='BalanceSheet', frequency='Quarterly')

            if df_is is None or df_bs is None or df_is.empty or df_bs.empty:
                continue

            # Các cột chứa chữ Q đại diện cho các quý (Q1 2024, Q2 2024...)
            periods = [col for col in df_is.columns if 'Q' in str(col).upper()]

            for period in periods:
                period_str = str(period).replace(' ', '-').upper()
                
                # Trích xuất dữ liệu gốc qua tiếng Việt
                revenue = extract_metric(df_is, ['Doanh thu thuần', 'Thu nhập lãi thuần'], period)
                profit = extract_metric(df_is, ['Lợi nhuận sau thuế'], period)
                assets = extract_metric(df_bs, ['Tổng cộng tài sản', 'Tổng tài sản'], period)
                liabilities = extract_metric(df_bs, ['Nợ phải trả'], period)
                equity = extract_metric(df_bs, ['Vốn chủ sở hữu'], period)

                if revenue is not None or assets is not None:
                    record = {
                        "ticker": ticker,
                        "report_type": "quarterly",
                        "period": period_str,
                        "revenue": revenue,
                        "profit_after_tax": profit,
                        "total_assets": assets,
                        "total_liabilities": liabilities,
                        "equity": equity,
                        "eps": None
                    }
                    all_financials.append(record)

            count += 1
            time.sleep(0.5) # Nghỉ để tránh API block (BCTC rất nặng)

            if len(all_financials) >= batch_size:
                supabase.table("financial_reports").upsert(
                    all_financials, 
                    on_conflict="ticker,report_type,period"
                ).execute()
                print(f"✅ Đã nạp báo cáo tài chính cho lô mới ({len(all_financials)} records). Tiến độ: {count}/{len(tickers)}")
                all_financials = []

        except Exception as e:
            # Các mã sinh lỗi vnstock sẽ được tự bỏ qua êm ái
            continue

    if all_financials:
        supabase.table("financial_reports").upsert(all_financials, on_conflict="ticker,report_type,period").execute()
        print(f"🏁 Đã hoàn tất nạp toàn bộ ({len(all_financials)}) Báo cáo tài chính cuối!")

if __name__ == "__main__":
    fetch_and_store_financials()
