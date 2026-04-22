-- Business-facing Vietnamese views
-- Keep raw tables unchanged to avoid breaking ETL/contracts.

-- 1) Industry check by stock (group-by-stock friendly view)
create or replace view vw_nganh_theo_ma as
select
    t.ticker as "Mã cổ phiếu",
    t.company_name as "Tên doanh nghiệp",
    t.exchange as "Sàn",
    t.industry as "Ngành",
    t.created_at as "Thời điểm tạo"
from tickers t;

-- 2) Financial report (meaningful fields only, Vietnamese aliases)
create or replace view vw_bao_cao_tai_chinh_vi as
select
    fr.ticker as "Mã cổ phiếu",
    fr.report_type as "Loại báo cáo",
    fr.period as "Kỳ báo cáo",

    -- Income statement core
    fr.revenue as "Doanh thu thuần (tỷ VND)",
    fr.gross_profit as "Lợi nhuận gộp (tỷ VND)",
    fr.operating_profit as "Lợi nhuận thuần HĐKD (tỷ VND)",
    fr.profit_before_tax as "Lợi nhuận trước thuế (tỷ VND)",
    fr.profit_after_tax as "Lợi nhuận sau thuế (tỷ VND)",
    fr.parent_profit_after_tax as "LNST công ty mẹ (tỷ VND)",
    fr.eps as "EPS",

    -- Balance sheet core
    fr.cash_and_cash_equivalents as "Tiền và tương đương tiền (tỷ VND)",
    fr.inventory as "Hàng tồn kho (tỷ VND)",
    fr.total_assets as "Tổng tài sản (tỷ VND)",
    fr.total_liabilities as "Tổng nợ phải trả (tỷ VND)",
    fr.owner_equity as "Vốn chủ sở hữu (tỷ VND)",
    fr.short_term_debt as "Nợ vay ngắn hạn (tỷ VND)",
    fr.long_term_debt as "Nợ vay dài hạn (tỷ VND)",

    -- Cashflow core
    fr.cash_flow_operating as "Lưu chuyển tiền thuần HĐKD (tỷ VND)",
    fr.cash_flow_investing as "Lưu chuyển tiền thuần HĐĐT (tỷ VND)",
    fr.cash_flow_financing as "Lưu chuyển tiền thuần HĐTC (tỷ VND)",
    fr.net_cash_flow as "Lưu chuyển tiền thuần trong kỳ (tỷ VND)",

    -- Key ratios
    fr.gross_margin as "Biên lợi nhuận gộp",
    fr.operating_margin as "Biên lợi nhuận hoạt động",
    fr.net_margin as "Biên lợi nhuận ròng",
    fr.roe as "ROE",
    fr.roa as "ROA",
    fr.debt_to_equity as "Nợ/Vốn chủ sở hữu",
    fr.current_ratio as "Hệ số thanh toán hiện hành",
    fr.asset_turnover as "Vòng quay tài sản",

    fr.created_at as "Thời điểm tạo"
from financial_reports fr;
