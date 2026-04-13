from vnstock import listing_companies
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

# 1. Cấu hình
db_uri = "postgresql://postgres:%26Y2FJ5L9nWxXtmM@db.xqargstnhajgdkfrneqb.supabase.co:5432/postgres"
engine = create_engine(db_uri)

def crawl_master_data():
    print("📡 Đang lấy danh sách 1.600 mã từ HOSE & HNX...")
    df = listing_companies()
    
    # In ra các cột hiện có để xem API trả về báo cáo cấu trúc thế nào
    print("Các cột dữ liệu trả về:", df.columns.tolist())
    
    # Tạo các cột mặc định nếu API bị thiếu để tránh lỗi KeyError
    if 'icbName3' not in df.columns:
        df['icbName3'] = 'N/A' # Ngành nghề bị thiếu
    if 'organName' not in df.columns:
        df['organName'] = df.get('organShortName', df['ticker'])
    if 'comGroupCode' not in df.columns:
        df['comGroupCode'] = 'UNKNOWN'

    # Chuẩn hóa cột
    df_master = df[['ticker', 'organName', 'icbName3', 'comGroupCode']].copy()
    df_master.columns = ['ticker', 'company_name', 'industry', 'exchange']
    
    # Upsert (Cập nhật nếu đã có, thêm mới nếu chưa)
    def postgres_upsert(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_stmt = insert(table.table).values(data)
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['ticker'],
            set_={c.name: c for c in insert_stmt.excluded if c.name != 'ticker'}
        )
        conn.execute(update_stmt)

    df_master.to_sql('company_info', engine, if_exists='append', index=False, method=postgres_upsert)
    print(f"✅ Đã nạp xong {len(df_master)} mã công ty vào 'company_info'.")

if __name__ == "__main__":
    crawl_master_data()
