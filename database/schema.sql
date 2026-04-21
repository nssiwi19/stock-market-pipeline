-- =========================================================================
-- E15: STOCK MARKET BIG DATA PIPELINE - DATABASE INITIALIZATION SCRIPT
-- RUN THIS SCRIPT IN YOUR SUPABASE SQL EDITOR
-- =========================================================================

-- 1. Table: tickers
-- Description: Stores baseline profile information for each stock.
CREATE TABLE IF NOT EXISTS tickers (
    ticker VARCHAR(10) PRIMARY KEY,
    exchange VARCHAR(10) NOT NULL, -- HOSE or HNX
    industry VARCHAR(255),
    company_name TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now())
);

-- 2. Table: daily_prices
-- Description: Time-series table for storing daily OHLCV (Adjusted) data
CREATE TABLE IF NOT EXISTS daily_prices (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    ticker VARCHAR(10) REFERENCES tickers(ticker),
    trading_date DATE NOT NULL,
    open_price NUMERIC(15, 2),
    high_price NUMERIC(15, 2),
    low_price NUMERIC(15, 2),
    close_price NUMERIC(15, 2), -- Adjusted Close
    volume BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()),
    CONSTRAINT check_price_logic CHECK (
        low_price <= high_price
        AND open_price BETWEEN low_price AND high_price
        AND close_price BETWEEN low_price AND high_price
        AND volume >= 0
    ),
    UNIQUE(ticker, trading_date) -- Composite unique constraint to prevent duplicates during upsert
);

-- Optimize queries by creating Index for filtering by Date and Ticker
CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices (trading_date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker ON daily_prices (ticker);

-- 3. Table: financial_reports
-- Description: Fundamental data (Quarterly and Annual)
CREATE TABLE IF NOT EXISTS financial_reports (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    ticker VARCHAR(10) REFERENCES tickers(ticker),
    report_type VARCHAR(20) NOT NULL, -- 'quarterly' or 'yearly'
    period VARCHAR(20) NOT NULL, -- e.g., 'Q1-2025' or '2025'
    
    -- Income Statement (ty VND, tru eps va ratios)
    revenue NUMERIC(20, 2),
    cogs NUMERIC(20, 2),
    gross_profit NUMERIC(20, 2),
    financial_income NUMERIC(20, 2),
    financial_expense NUMERIC(20, 2),
    interest_expense NUMERIC(20, 2),
    selling_expense NUMERIC(20, 2),
    general_admin_expense NUMERIC(20, 2),
    operating_profit NUMERIC(20, 2),
    other_income NUMERIC(20, 2),
    other_expense NUMERIC(20, 2),
    profit_before_tax NUMERIC(20, 2),
    profit_after_tax NUMERIC(20, 2),
    parent_profit_after_tax NUMERIC(20, 2),
    minority_profit NUMERIC(20, 2),
    depreciation_amortization NUMERIC(20, 2),
    ebit NUMERIC(20, 2),
    ebitda NUMERIC(20, 2),
    eps NUMERIC(20, 4),

    -- Balance Sheet
    cash_and_cash_equivalents NUMERIC(20, 2),
    short_term_investments NUMERIC(20, 2),
    short_term_receivables NUMERIC(20, 2),
    inventory NUMERIC(20, 2),
    other_current_assets NUMERIC(20, 2),
    total_current_assets NUMERIC(20, 2),
    long_term_receivables NUMERIC(20, 2),
    fixed_assets NUMERIC(20, 2),
    investment_properties NUMERIC(20, 2),
    long_term_assets NUMERIC(20, 2),
    total_assets NUMERIC(20, 2),
    short_term_debt NUMERIC(20, 2),
    accounts_payable NUMERIC(20, 2),
    short_term_liabilities NUMERIC(20, 2),
    total_short_term_liabilities NUMERIC(20, 2),
    long_term_debt NUMERIC(20, 2),
    total_long_term_liabilities NUMERIC(20, 2),
    total_liabilities NUMERIC(20, 2),
    owner_equity NUMERIC(20, 2),
    equity NUMERIC(20, 2),
    retained_earnings NUMERIC(20, 2),
    share_capital NUMERIC(20, 2),
    total_equity_and_liabilities NUMERIC(20, 2),

    -- Cashflow
    cash_flow_operating NUMERIC(20, 2),
    cash_flow_investing NUMERIC(20, 2),
    cash_flow_financing NUMERIC(20, 2),
    net_cash_flow NUMERIC(20, 2),
    capex NUMERIC(20, 2),

    -- Ratios
    gross_margin NUMERIC(20, 6),
    operating_margin NUMERIC(20, 6),
    net_margin NUMERIC(20, 6),
    roe NUMERIC(20, 6),
    roa NUMERIC(20, 6),
    debt_to_equity NUMERIC(20, 6),
    current_ratio NUMERIC(20, 6),
    asset_turnover NUMERIC(20, 6),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()),
    UNIQUE(ticker, report_type, period)
);

CREATE INDEX IF NOT EXISTS idx_financial_reports_ticker ON financial_reports (ticker);

-- Note: Ensure that the 'pgcrypto' or 'uuid-ossp' extension is enabled in Supabase 
-- for uuid_generate_v4() to work (it is enabled by default on new projects).
