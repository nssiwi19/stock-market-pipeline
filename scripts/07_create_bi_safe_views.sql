-- BI-safe layer for Supabase SQL Editor
-- Goal: use only trusted-source rows, no OCR/doc source, and yearly latest-per-ticker records.

create or replace view public.vw_financial_reports_bi_safe as
with base as (
  select
    fr.*,
    case
      when fr.period ~ '.*[0-9]{4}.*'
        then substring(fr.period from '([0-9]{4})')::int
      else null
    end as period_year
  from public.financial_reports fr
  where coalesce(trim(fr.source), '') <> ''
    and not (string_to_array(fr.source, '+') @> array['vietstock_bctc_documents'])
    and (string_to_array(fr.source, '+') && array['cafef_requests','vietstock_financeinfo','cafef_cloudscraper'])
    and coalesce(abs(fr.revenue), 0) <= 10000000
    and coalesce(abs(fr.profit_after_tax), 0) <= 10000000
    and coalesce(abs(fr.total_assets), 0) <= 10000000
    and coalesce(abs(fr.total_liabilities), 0) <= 10000000
    and coalesce(abs(fr.equity), 0) <= 10000000
    and coalesce(abs(fr.eps), 0) <= 100000
),
ranked as (
  select
    b.*,
    row_number() over (
      partition by b.ticker, b.report_type
      order by b.period_year desc nulls last, b.created_at desc
    ) as rn_in_type
  from base b
)
select *
from ranked;

create or replace view public.vw_bi_latest_yearly_safe as
select
  r.ticker,
  t.industry,
  r.period,
  r.period_year,
  r.revenue,
  r.profit_after_tax,
  r.source,
  r.confidence,
  r.created_at
from public.vw_financial_reports_bi_safe r
left join public.tickers t on t.ticker = r.ticker
where r.report_type = 'yearly'
  and r.rn_in_type = 1
  and coalesce(trim(t.industry), '') <> '';

-- Chart 1: Top ngành theo doanh thu
create or replace view public.vw_bi_top_industry_revenue_safe as
select
  industry,
  count(*) as ticker_count,
  round(sum(revenue)::numeric, 2) as total_revenue_bn
from public.vw_bi_latest_yearly_safe
where revenue is not null
  and revenue >= 0
group by industry
order by total_revenue_bn desc;

-- Chart 2: Top 5 ngành theo lợi nhuận
create or replace view public.vw_bi_top5_industry_profit_safe as
select
  industry,
  count(*) as ticker_count,
  round(sum(profit_after_tax)::numeric, 2) as total_profit_after_tax_bn
from public.vw_bi_latest_yearly_safe
where profit_after_tax is not null
group by industry
order by total_profit_after_tax_bn desc
limit 5;
