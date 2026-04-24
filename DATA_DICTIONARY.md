# Data Dictionary

Tai lieu nay mo ta y nghia va don vi cac truong chinh trong Supabase cho du an `stock-market-pipeline`.

## Quy Uoc Don Vi

- Gia co phieu: `VND / co phieu`
- Khoi luong giao dich: `co phieu`
- Chi tieu tai chinh tien te trong `financial_reports`: `ty VND` (da chia `1e9` trong ETL), tru `eps`
- `eps`: don vi goc tu nguon (thuc te thuong la `VND / co phieu`)
- Ratio (`margin`, `roe`, `roa`, ...): so thap phan khong don vi (vi du `0.15` = `15%`)
- Thoi gian: `TIMESTAMP WITH TIME ZONE` (UTC)

## Bang `tickers`

| Column | Type | Unit | Mo ta |
|---|---|---|---|
| `ticker` | `VARCHAR(10)` | - | Ma chung khoan, khoa chinh |
| `exchange` | `VARCHAR(10)` | - | San giao dich (`HOSE`, `HNX`, `UPCOM`, `UNKNOWN`) |
| `industry` | `VARCHAR(255)` | - | Nhom nganh chuan dung cho BI |
| `company_name` | `TEXT` | - | Ten doanh nghiep |
| `contact_phone` | `VARCHAR(50)` | - | So dien thoai lien he doanh nghiep (neu co) |
| `created_at` | `TIMESTAMPTZ` | UTC | Thoi diem tao record |
| `industry_inferred` | `TEXT` | - | Nganh suy luan (migration 02) |
| `industry_inferred_confidence` | `NUMERIC(6,4)` | 0-1 | Do tin cay suy luan |
| `industry_inferred_method` | `VARCHAR(50)` | - | Cach suy luan (`rule_keyword`, `ens_agree`, ...) |
| `industry_inferred_at` | `TIMESTAMPTZ` | UTC | Thoi diem suy luan |

## Bang `daily_prices`

| Column | Type | Unit | Mo ta |
|---|---|---|---|
| `id` | `UUID` | - | Khoa chinh |
| `ticker` | `VARCHAR(10)` | - | Ma CK, FK sang `tickers` |
| `trading_date` | `DATE` | - | Ngay giao dich |
| `open_price` | `NUMERIC(15,2)` | VND/co phieu | Gia mo cua |
| `high_price` | `NUMERIC(15,2)` | VND/co phieu | Gia cao nhat |
| `low_price` | `NUMERIC(15,2)` | VND/co phieu | Gia thap nhat |
| `close_price` | `NUMERIC(15,2)` | VND/co phieu | Gia dong cua (adjusted close) |
| `volume` | `BIGINT` | co phieu | Khoi luong |
| `created_at` | `TIMESTAMPTZ` | UTC | Thoi diem ghi nhan |

## Bang `financial_reports`

### Khoa va metadata

| Column | Type | Unit | Mo ta |
|---|---|---|---|
| `id` | `UUID` | - | Khoa chinh |
| `ticker` | `VARCHAR(10)` | - | Ma CK, FK sang `tickers` |
| `report_type` | `VARCHAR(20)` | - | `yearly` hoac `quarterly` |
| `period` | `VARCHAR(20)` | - | Ky bao cao (`FY-2025`, `Q1-2025`, ...) |
| `created_at` | `TIMESTAMPTZ` | UTC | Thoi diem ghi nhan |
| `source` | `TEXT` | - | Nguon du lieu goc (`cafef_requests`, `vietstock_financeinfo`, `vietstock_bctc_documents`, ...) |
| `confidence` | `NUMERIC` | 0-1 | Do tin cay record tong hop |

### Income Statement (ty VND, tru `eps`)

`revenue`, `cogs`, `gross_profit`, `financial_income`, `financial_expense`, `interest_expense`,
`selling_expense`, `general_admin_expense`, `operating_profit`, `other_income`, `other_expense`,
`profit_before_tax`, `profit_after_tax`, `parent_profit_after_tax`, `minority_profit`,
`depreciation_amortization`, `ebit`, `ebitda` -> `ty VND`

`eps` -> don vi goc tu nguon (thuong `VND / co phieu`)

### Balance Sheet (ty VND)

`cash_and_cash_equivalents`, `short_term_investments`, `short_term_receivables`, `inventory`,
`other_current_assets`, `total_current_assets`, `long_term_receivables`, `fixed_assets`,
`investment_properties`, `long_term_assets`, `total_assets`, `short_term_debt`,
`accounts_payable`, `short_term_liabilities`, `total_short_term_liabilities`,
`long_term_debt`, `total_long_term_liabilities`, `total_liabilities`, `owner_equity`,
`equity`, `retained_earnings`, `share_capital`, `total_equity_and_liabilities`

### Cashflow (ty VND)

`cash_flow_operating`, `cash_flow_investing`, `cash_flow_financing`, `net_cash_flow`, `capex`

### Ratios (khong don vi, dang so thap phan)

`gross_margin`, `operating_margin`, `net_margin`, `roe`, `roa`, `debt_to_equity`, `current_ratio`, `asset_turnover`

## Luu y cho BI

- Khi hien thi `%`, can nhan `100` cho cac cot ratio (vd `net_margin`).
- Nen dung `COALESCE(industry, industry_inferred)` trong dashboard phan tich nganh neu can mo rong coverage.
- Khong thay `NULL` bang `0` cho cot tien te neu nguon khong co du lieu (de tranh sai nghia nghiep vu).

## Bang `financial_reports_bi_gold`

Bang "vang" cho BI, da loc OCR/untrusted, da dung rule sanity va bo sung score chat luong.

| Column | Type | Unit | Mo ta |
|---|---|---|---|
| `ticker` | `TEXT` | - | Ma CK |
| `industry` | `TEXT` | - | Nganh goc |
| `industry_normalized` | `TEXT` | - | Nganh chuan hoa de aggregate (vd gom nhom Ngan hang) |
| `report_type` | `TEXT` | - | `yearly` hoac `quarterly` |
| `period` | `TEXT` | - | Ky bao cao |
| `period_year` | `INT` | nam | Nam trich tu `period` |
| `source` | `TEXT` | - | Chuoi nguon dong gop |
| `source_tier` | `TEXT` | - | Nhom nguon (`trusted_tagged`, `legacy_unknown`, `rejected`) |
| `confidence` | `NUMERIC` | 0-1 | Do tin cay |
| `revenue` | `NUMERIC` | ty VND | Doanh thu thuần |
| `profit_after_tax` | `NUMERIC` | ty VND | LNST |
| `eps` | `NUMERIC` | VND/co phieu | EPS da qua rule sanity |
| `gross_margin` | `NUMERIC` | decimal | Bien loi nhuan gop |
| `net_margin` | `NUMERIC` | decimal | Bien loi nhuan rong |
| `roe` | `NUMERIC` | decimal | LNST/Von chu |
| `roa` | `NUMERIC` | decimal | LNST/Tong tai san |
| `quality_score` | `INT` | 0-100 | Diem tong hop |
| `quality_score_revenue` | `INT` | 0-100 | Diem phu hop chart doanh thu |
| `quality_score_profit` | `INT` | 0-100 | Diem phu hop chart loi nhuan |
| `quality_note` | `TEXT` | - | `excellent`, `good`, `fair`, `weak` |
| `is_latest_yearly` | `BOOLEAN` | - | Co phai dong yearly moi nhat cua ma |
| `bi_ready_revenue` | `BOOLEAN` | - | Du dieu kien vao chart doanh thu |
| `bi_ready_profit` | `BOOLEAN` | - | Du dieu kien vao chart loi nhuan |
| `refreshed_at` | `TIMESTAMPTZ` | UTC | Lan refresh gan nhat |

## Bang `financial_reports_bi_gold_latest_yearly`

Bang materialized boi function refresh, gom 1 dong latest yearly cho moi ma.

- Muc dich: dung drill-down, table detail tren dashboard.
- Khoa chinh: `ticker`.
- Nguon: duoc tao tu `financial_reports_bi_gold` voi dieu kien `is_latest_yearly = true`.

## Bang `financial_reports_bi_gold_industry_agg`

Bang aggregate theo nganh de ve chart top nhanh va on dinh.

| Column | Type | Mo ta |
|---|---|---|
| `industry_normalized` | `TEXT` | Nganh chuan hoa |
| `metric` | `TEXT` | `revenue` hoac `profit` |
| `ticker_count` | `INT` | So ma dong gop vao tong |
| `total_value_bn` | `NUMERIC` | Tong gia tri theo metric (ty VND) |
| `min_ticker_gate` | `BOOLEAN` | Co dat nguong `ticker_count >= 5` |
| `refreshed_at` | `TIMESTAMPTZ` | Thoi diem cap nhat |

## Bang nen dung de ve BI

- Uu tien 1: `financial_reports_bi_gold_industry_agg` (chart top nganh).
- Uu tien 2: `financial_reports_bi_gold_latest_yearly` (drill-down theo ma).
- Han che dung truc tiep `financial_reports` cho dashboard final.
