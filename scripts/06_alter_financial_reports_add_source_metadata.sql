-- Add source metadata columns for financial_reports backfill governance
ALTER TABLE financial_reports
    ADD COLUMN IF NOT EXISTS source TEXT,
    ADD COLUMN IF NOT EXISTS confidence NUMERIC(5,4);

CREATE INDEX IF NOT EXISTS idx_financial_reports_source
    ON financial_reports (source);
