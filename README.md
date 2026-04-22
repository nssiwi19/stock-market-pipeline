
# Vietnam Stock Market Pipeline

Pipeline ETL cho du lieu chung khoan Viet Nam, luu tren Supabase Postgres va phuc vu dashboard BI.

## Muc tieu hien tai

- Thu thap va cap nhat du lieu `daily_prices`, `tickers`, `financial_reports`.
- Lam sach du lieu tai chinh theo trusted source (khong OCR cho BI).
- Cung cap 1 lop du lieu "gold" on dinh de ve BI nganh doanh thu/loi nhuan.

## Kien truc tong quan

- Nguon du lieu: CafeF, Vietstock (structured), cac script ETL Python.
- Kho du lieu: Supabase PostgreSQL.
- Lop BI:
- `financial_reports_bi_gold` (bang vang chi tiet).
- `financial_reports_bi_gold_latest_yearly` (1 dong latest yearly moi ma).
- `financial_reports_bi_gold_industry_agg` (tong hop theo nganh cho chart).

## Cac migration/script quan trong

- `scripts/04_create_execute_readonly_sql_rpc.sql`: RPC doc-only cho QA/telegram.
- `scripts/05_create_business_views_vi.sql`: cac view tieng Viet phuc vu truy van doc.
- `scripts/06_alter_financial_reports_add_source_metadata.sql`: bo sung metadata nguon.
- `scripts/07_create_bi_safe_views.sql`: lop view BI-safe.
- `scripts/08_create_bi_gold_table.sql`: tao bang gold + function refresh.

## Cach dung lop BI Gold

1. Chay file `scripts/08_create_bi_gold_table.sql` trong Supabase SQL Editor.
1. Refresh bang vang:

```sql
select public.refresh_financial_reports_bi_gold();
```

1. Ve dashboard tu bang aggregate:

```sql
select industry_normalized, ticker_count, total_value_bn
from public.financial_reports_bi_gold_industry_agg
where metric = 'revenue' and min_ticker_gate = true
order by total_value_bn desc;
```

```sql
select industry_normalized, ticker_count, total_value_bn
from public.financial_reports_bi_gold_industry_agg
where metric = 'profit' and min_ticker_gate = true
order by total_value_bn desc;
```

## Telegram Q&A Bot

Chay bot:

```bash
python scripts/telegram_qa_bot.py
```

Yeu cau:

- Da apply `scripts/04_create_execute_readonly_sql_rpc.sql`.
- Co cac bien moi truong:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID` (khuyen nghi)
- `GEMINI_API_KEY`

## Ghi chu van hanh

- BI nen doc tu bang gold (`financial_reports_bi_gold_*`) thay vi bang raw.
- Khong thay `NULL` bang `0` voi metric tai chinh.
- Ratio (`margin`, `roe`, `roa`) luu dang so thap phan, nhan `100` neu hien thi `%`.
