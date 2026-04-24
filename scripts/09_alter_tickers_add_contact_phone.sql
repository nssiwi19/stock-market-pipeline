-- Add contact phone column for ticker profile and industry mapping use-cases.
ALTER TABLE public.tickers
ADD COLUMN IF NOT EXISTS contact_phone VARCHAR(50);

