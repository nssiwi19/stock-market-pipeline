-- Rule-based/ML imputation lineage table for financial_reports
CREATE TABLE IF NOT EXISTS financial_reports_imputation_log (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    run_id TEXT NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    report_type VARCHAR(20) NOT NULL,
    period VARCHAR(20) NOT NULL,
    column_name TEXT NOT NULL,
    imputed_value TEXT,
    method TEXT NOT NULL,
    confidence NUMERIC(5,4) NOT NULL DEFAULT 1.0000,
    source_columns TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now())
);

CREATE INDEX IF NOT EXISTS idx_fr_imputation_log_run_id
    ON financial_reports_imputation_log (run_id);

CREATE INDEX IF NOT EXISTS idx_fr_imputation_log_ticker_period
    ON financial_reports_imputation_log (ticker, report_type, period);
