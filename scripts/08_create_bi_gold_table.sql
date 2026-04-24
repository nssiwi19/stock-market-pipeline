-- One "gold" table for BI
-- Run this file in Supabase SQL Editor.
-- After that, dashboard should read from public.financial_reports_bi_gold only.

create table if not exists public.financial_reports_bi_gold (
  ticker text not null,
  industry text,
  industry_normalized text,
  report_type text not null,
  period text not null,
  period_year int,
  source text,
  confidence numeric,
  source_tier text,
  created_at timestamptz,

  revenue numeric,
  cogs numeric,
  gross_profit numeric,
  operating_profit numeric,
  profit_before_tax numeric,
  profit_after_tax numeric,
  eps numeric,
  total_assets numeric,
  total_liabilities numeric,
  equity numeric,

  gross_margin numeric,
  operating_margin numeric,
  net_margin numeric,
  roe numeric,
  roa numeric,
  debt_to_equity numeric,
  asset_turnover numeric,

  quality_score int not null default 0,
  quality_score_revenue int not null default 0,
  quality_score_profit int not null default 0,
  quality_note text,
  is_latest_yearly boolean not null default false,
  bi_ready_revenue boolean not null default false,
  bi_ready_profit boolean not null default false,
  refreshed_at timestamptz not null default now(),
  primary key (ticker, report_type, period)
);

alter table public.financial_reports_bi_gold add column if not exists source_tier text;
alter table public.financial_reports_bi_gold add column if not exists industry_normalized text;
alter table public.financial_reports_bi_gold add column if not exists is_latest_yearly boolean not null default false;
alter table public.financial_reports_bi_gold add column if not exists bi_ready_revenue boolean not null default false;
alter table public.financial_reports_bi_gold add column if not exists bi_ready_profit boolean not null default false;
alter table public.financial_reports_bi_gold add column if not exists quality_score_revenue int not null default 0;
alter table public.financial_reports_bi_gold add column if not exists quality_score_profit int not null default 0;

create index if not exists idx_financial_reports_bi_gold_industry on public.financial_reports_bi_gold(industry);
create index if not exists idx_financial_reports_bi_gold_industry_norm on public.financial_reports_bi_gold(industry_normalized);
create index if not exists idx_financial_reports_bi_gold_period_year on public.financial_reports_bi_gold(period_year);
create index if not exists idx_financial_reports_bi_gold_report_type on public.financial_reports_bi_gold(report_type);

create table if not exists public.financial_reports_bi_gold_latest_yearly (
  ticker text primary key,
  industry text,
  industry_normalized text,
  period text,
  period_year int,
  source text,
  source_tier text,
  confidence numeric,
  created_at timestamptz,
  revenue numeric,
  profit_after_tax numeric,
  quality_score int not null default 0,
  quality_score_revenue int not null default 0,
  quality_score_profit int not null default 0,
  bi_ready_revenue boolean not null default false,
  bi_ready_profit boolean not null default false,
  refreshed_at timestamptz not null default now()
);

create index if not exists idx_fr_bi_gold_latest_industry_norm on public.financial_reports_bi_gold_latest_yearly(industry_normalized);

create table if not exists public.financial_reports_bi_gold_industry_agg (
  industry_normalized text not null,
  metric text not null,
  ticker_count int not null,
  total_value_bn numeric,
  min_ticker_gate boolean not null,
  refreshed_at timestamptz not null default now(),
  primary key (industry_normalized, metric)
);

create index if not exists idx_fr_bi_gold_industry_agg_metric on public.financial_reports_bi_gold_industry_agg(metric);

create or replace function public.refresh_financial_reports_bi_gold()
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  truncate table public.financial_reports_bi_gold;

  insert into public.financial_reports_bi_gold (
    ticker,
    industry,
    industry_normalized,
    report_type,
    period,
    period_year,
    source,
    confidence,
    source_tier,
    created_at,
    revenue,
    cogs,
    gross_profit,
    operating_profit,
    profit_before_tax,
    profit_after_tax,
    eps,
    total_assets,
    total_liabilities,
    equity,
    gross_margin,
    operating_margin,
    net_margin,
    roe,
    roa,
    debt_to_equity,
    asset_turnover,
    quality_score,
    quality_score_revenue,
    quality_score_profit,
    quality_note,
    is_latest_yearly,
    bi_ready_revenue,
    bi_ready_profit,
    refreshed_at
  )
  with base as (
    select
      fr.ticker,
      t.industry,
      case
        when lower(coalesce(t.industry, '')) like '%ngân hàng%' then 'Ngân hàng'
        when lower(coalesce(t.industry, '')) like '%bank%' then 'Ngân hàng'
        when lower(coalesce(t.industry, '')) like '%bảo hiểm%' then 'Bảo hiểm'
        when lower(coalesce(t.industry, '')) like '%chứng khoán%' then 'Chứng khoán'
        else t.industry
      end as industry_normalized,
      fr.report_type,
      fr.period,
      case
        when fr.period ~ '.*[0-9]{4}.*'
          then substring(fr.period from '([0-9]{4})')::int
        else null
      end as period_year,
      fr.source,
      fr.confidence,
      case
        when coalesce(trim(fr.source), '') = '' then 'legacy_unknown'
        when string_to_array(fr.source, '+') && array['cafef_requests', 'vietstock_financeinfo', 'cafef_cloudscraper']
          then 'trusted_tagged'
        else 'rejected'
      end as source_tier,
      fr.created_at,

      -- Keep only sane ranges for core metrics
      case when abs(fr.revenue) <= 10000000 then fr.revenue else null end as revenue,
      case when abs(fr.cogs) <= 10000000 then fr.cogs else null end as cogs,
      case when abs(fr.gross_profit) <= 10000000 then fr.gross_profit else null end as gross_profit,
      case when abs(fr.operating_profit) <= 10000000 then fr.operating_profit else null end as operating_profit,
      case when abs(fr.profit_before_tax) <= 10000000 then fr.profit_before_tax else null end as profit_before_tax,
      case when abs(fr.profit_after_tax) <= 10000000 then fr.profit_after_tax else null end as profit_after_tax,
      case when abs(fr.eps) <= 100000 then fr.eps else null end as eps,
      case when abs(fr.total_assets) <= 10000000 then fr.total_assets else null end as total_assets,
      case when abs(fr.total_liabilities) <= 10000000 then fr.total_liabilities else null end as total_liabilities,
      case when abs(fr.equity) <= 10000000 then fr.equity else null end as equity
    from public.financial_reports fr
    left join public.tickers t on t.ticker = fr.ticker
    where not (coalesce(fr.source, '') like '%vietstock_bctc_documents%')
      and (
        coalesce(trim(fr.source), '') = ''
        or (string_to_array(fr.source, '+') && array['cafef_requests', 'vietstock_financeinfo', 'cafef_cloudscraper'])
      )
      and coalesce(trim(t.industry), '') <> ''
  ),
  ratio_calc as (
    select
      b.*,
      case when b.revenue is not null and b.revenue <> 0 and b.gross_profit is not null
        then b.gross_profit / b.revenue * 100 else null end as gross_margin_raw,
      case when b.revenue is not null and b.revenue <> 0 and b.operating_profit is not null
        then b.operating_profit / b.revenue * 100 else null end as operating_margin_raw,
      case when b.revenue is not null and b.revenue <> 0 and b.profit_after_tax is not null
        then b.profit_after_tax / b.revenue * 100 else null end as net_margin_raw,
      case when b.equity is not null and b.equity <> 0 and b.profit_after_tax is not null
        then b.profit_after_tax / b.equity * 100 else null end as roe_raw,
      case when b.total_assets is not null and b.total_assets <> 0 and b.profit_after_tax is not null
        then b.profit_after_tax / b.total_assets * 100 else null end as roa_raw,
      case when b.equity is not null and b.equity <> 0 and b.total_liabilities is not null
        then b.total_liabilities / b.equity else null end as debt_to_equity_raw,
      case when b.total_assets is not null and b.total_assets <> 0 and b.revenue is not null
        then b.revenue / b.total_assets else null end as asset_turnover_raw
    from base b
  ),
  normalized as (
    select
      r.ticker,
      r.industry,
      r.industry_normalized,
      r.report_type,
      r.period,
      r.period_year,
      r.source,
      r.confidence,
      r.source_tier,
      r.created_at,
      r.revenue,
      r.cogs,
      r.gross_profit,
      r.operating_profit,
      r.profit_before_tax,
      r.profit_after_tax,
      r.eps,
      r.total_assets,
      r.total_liabilities,
      r.equity,
      case when r.gross_margin_raw is not null and abs(r.gross_margin_raw) <= 100 then r.gross_margin_raw else null end as gross_margin,
      case when r.operating_margin_raw is not null and abs(r.operating_margin_raw) <= 100 then r.operating_margin_raw else null end as operating_margin,
      case when r.net_margin_raw is not null and abs(r.net_margin_raw) <= 100 then r.net_margin_raw else null end as net_margin,
      case when r.roe_raw is not null and abs(r.roe_raw) <= 100 then r.roe_raw else null end as roe,
      case when r.roa_raw is not null and abs(r.roa_raw) <= 100 then r.roa_raw else null end as roa,
      case when r.debt_to_equity_raw is not null and abs(r.debt_to_equity_raw) <= 100 then r.debt_to_equity_raw else null end as debt_to_equity,
      case when r.asset_turnover_raw is not null and abs(r.asset_turnover_raw) <= 100 then r.asset_turnover_raw else null end as asset_turnover,
      (
        case when r.revenue is not null then 35 else 0 end +
        case when r.profit_after_tax is not null then 35 else 0 end +
        case when r.period_year is not null then 10 else 0 end +
        case when coalesce(trim(r.industry), '') <> '' then 10 else 0 end +
        case when r.source_tier = 'trusted_tagged' then 10 else 5 end
      )::int as quality_score,
      (
        case when r.revenue is not null and r.revenue >= 0 then 50 else 0 end +
        case when r.period_year is not null then 20 else 0 end +
        case when coalesce(trim(r.industry_normalized), '') <> '' then 15 else 0 end +
        case when r.source_tier = 'trusted_tagged' then 15 else 5 end
      )::int as quality_score_revenue,
      (
        case when r.profit_after_tax is not null then 50 else 0 end +
        case when r.period_year is not null then 20 else 0 end +
        case when coalesce(trim(r.industry_normalized), '') <> '' then 15 else 0 end +
        case when r.source_tier = 'trusted_tagged' then 15 else 5 end
      )::int as quality_score_profit,
      now() as refreshed_at
    from ratio_calc r
  ),
  ranked as (
    select
      n.*,
      row_number() over (
        partition by n.ticker, n.report_type
        order by n.period_year desc nulls last, n.created_at desc
      ) as rn_in_type
    from normalized n
  )
  select
    n.ticker,
    n.industry,
    n.industry_normalized,
    n.report_type,
    n.period,
    n.period_year,
    n.source,
    n.confidence,
    n.source_tier,
    n.created_at,
    n.revenue,
    n.cogs,
    n.gross_profit,
    n.operating_profit,
    n.profit_before_tax,
    n.profit_after_tax,
    n.eps,
    n.total_assets,
    n.total_liabilities,
    n.equity,
    n.gross_margin,
    n.operating_margin,
    n.net_margin,
    n.roe,
    n.roa,
    n.debt_to_equity,
    n.asset_turnover,
    n.quality_score,
    n.quality_score_revenue,
    n.quality_score_profit,
    case
      when n.quality_score >= 90 then 'excellent'
      when n.quality_score >= 75 then 'good'
      when n.quality_score >= 40 then 'fair'
      else 'weak'
    end as quality_note,
    (n.report_type = 'yearly' and n.rn_in_type = 1) as is_latest_yearly,
    ((n.report_type = 'yearly' and n.rn_in_type = 1) and n.revenue is not null and n.revenue >= 0 and n.quality_score_revenue >= 75) as bi_ready_revenue,
    ((n.report_type = 'yearly' and n.rn_in_type = 1) and n.profit_after_tax is not null and n.quality_score_profit >= 75) as bi_ready_profit,
    n.refreshed_at
  from ranked n
  where n.source_tier <> 'rejected';

  truncate table public.financial_reports_bi_gold_latest_yearly;
  insert into public.financial_reports_bi_gold_latest_yearly (
    ticker,
    industry,
    industry_normalized,
    period,
    period_year,
    source,
    source_tier,
    confidence,
    created_at,
    revenue,
    profit_after_tax,
    quality_score,
    quality_score_revenue,
    quality_score_profit,
    bi_ready_revenue,
    bi_ready_profit,
    refreshed_at
  )
  select
    g.ticker,
    g.industry,
    g.industry_normalized,
    g.period,
    g.period_year,
    g.source,
    g.source_tier,
    g.confidence,
    g.created_at,
    g.revenue,
    g.profit_after_tax,
    g.quality_score,
    g.quality_score_revenue,
    g.quality_score_profit,
    g.bi_ready_revenue,
    g.bi_ready_profit,
    now()
  from public.financial_reports_bi_gold g
  where g.is_latest_yearly = true;

  truncate table public.financial_reports_bi_gold_industry_agg;
  insert into public.financial_reports_bi_gold_industry_agg (
    industry_normalized,
    metric,
    ticker_count,
    total_value_bn,
    min_ticker_gate,
    refreshed_at
  )
  with latest_revenue_per_ticker as (
    select
      g.ticker,
      g.industry_normalized,
      g.revenue,
      row_number() over (
        partition by g.ticker
        order by g.period_year desc nulls last, g.created_at desc
      ) as rn
    from public.financial_reports_bi_gold g
    where g.report_type = 'yearly'
      and g.revenue is not null
      and g.revenue >= 0
      and g.quality_score_revenue >= 75
  ),
  latest_profit_per_ticker as (
    select
      g.ticker,
      g.industry_normalized,
      g.profit_after_tax,
      row_number() over (
        partition by g.ticker
        order by g.period_year desc nulls last, g.created_at desc
      ) as rn
    from public.financial_reports_bi_gold g
    where g.report_type = 'yearly'
      and g.profit_after_tax is not null
      and g.quality_score_profit >= 75
  )
  select
    coalesce(nullif(trim(r.industry_normalized), ''), 'UNKNOWN') as industry_normalized,
    'revenue'::text as metric,
    count(*)::int as ticker_count,
    round(sum(r.revenue)::numeric, 2) as total_value_bn,
    (count(*) >= 5) as min_ticker_gate,
    now()
  from latest_revenue_per_ticker r
  where r.rn = 1
  group by 1
  union all
  select
    coalesce(nullif(trim(p.industry_normalized), ''), 'UNKNOWN') as industry_normalized,
    'profit'::text as metric,
    count(*)::int as ticker_count,
    round(sum(p.profit_after_tax)::numeric, 2) as total_value_bn,
    (count(*) >= 3) as min_ticker_gate,
    now()
  from latest_profit_per_ticker p
  where p.rn = 1
  group by 1;
end;
$$;

-- First refresh
select public.refresh_financial_reports_bi_gold();
