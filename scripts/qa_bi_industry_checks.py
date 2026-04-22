import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT_ROOT / "scripts" / "output"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import get_supabase_client


def run_sql(sql: str) -> list[dict[str, Any]]:
    supabase = get_supabase_client()
    resp = supabase.rpc("execute_readonly_sql", {"p_sql": sql}).execute()
    data = resp.data or []
    if not isinstance(data, list):
        return []
    return data


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    sqls = {
        "industry_completeness": """
            select
                count(*) as total_tickers,
                sum(case when coalesce(trim(industry), '') = '' then 1 else 0 end) as missing_industry,
                round(
                    100.0 * sum(case when coalesce(trim(industry), '') = '' then 1 else 0 end) / nullif(count(*), 0),
                    2
                ) as missing_industry_pct
            from tickers
        """,
        "duplicate_financial_keys": """
            select
                ticker,
                report_type,
                period,
                count(*) as duplicate_count
            from financial_reports
            group by ticker, report_type, period
            having count(*) > 1
            order by duplicate_count desc, ticker asc
            limit 100
        """,
        "latest_yearly_quality": """
            with ranked as (
                select
                    fr.ticker,
                    fr.period,
                    fr.revenue,
                    fr.profit_after_tax,
                    t.industry,
                    row_number() over (
                        partition by fr.ticker
                        order by
                            case when fr.period ~ '.*[0-9]{4}.*' then substring(fr.period from '([0-9]{4})')::int else 0 end desc,
                            fr.created_at desc
                    ) as rn
                from financial_reports fr
                left join tickers t on t.ticker = fr.ticker
                where fr.report_type = 'yearly'
            )
            select
                count(*) as latest_ticker_rows,
                sum(case when coalesce(trim(industry), '') = '' then 1 else 0 end) as latest_missing_industry,
                sum(case when revenue is null then 1 else 0 end) as latest_missing_revenue,
                sum(case when profit_after_tax is null then 1 else 0 end) as latest_missing_profit_after_tax,
                sum(case when revenue < 0 then 1 else 0 end) as latest_negative_revenue,
                sum(case when period !~ '.*[0-9]{4}.*' then 1 else 0 end) as latest_invalid_period_format,
                sum(case when period ~ '.*[0-9]{4}.*' and substring(period from '([0-9]{4})')::int < 2023 then 1 else 0 end) as latest_stale_before_2023
            from ranked
            where rn = 1
        """,
        "top_industry_revenue_latest": """
            with ranked as (
                select
                    fr.ticker,
                    fr.period,
                    fr.revenue,
                    t.industry,
                    row_number() over (
                        partition by fr.ticker
                        order by
                            case when fr.period ~ '.*[0-9]{4}.*' then substring(fr.period from '([0-9]{4})')::int else 0 end desc,
                            fr.created_at desc
                    ) as rn
                from financial_reports fr
                left join tickers t on t.ticker = fr.ticker
                where fr.report_type = 'yearly'
            )
            select
                industry,
                count(*) as ticker_count,
                round(sum(revenue)::numeric, 2) as total_revenue_bn
            from ranked
            where rn = 1
              and coalesce(trim(industry), '') <> ''
              and revenue is not null
            group by industry
            order by total_revenue_bn desc
            limit 30
        """,
        "top5_industry_profit_latest": """
            with ranked as (
                select
                    fr.ticker,
                    fr.period,
                    fr.profit_after_tax,
                    t.industry,
                    row_number() over (
                        partition by fr.ticker
                        order by
                            case when fr.period ~ '.*[0-9]{4}.*' then substring(fr.period from '([0-9]{4})')::int else 0 end desc,
                            fr.created_at desc
                    ) as rn
                from financial_reports fr
                left join tickers t on t.ticker = fr.ticker
                where fr.report_type = 'yearly'
            )
            select
                industry,
                count(*) as ticker_count,
                round(sum(profit_after_tax)::numeric, 2) as total_profit_after_tax_bn
            from ranked
            where rn = 1
              and coalesce(trim(industry), '') <> ''
              and profit_after_tax is not null
            group by industry
            order by total_profit_after_tax_bn desc
            limit 5
        """,
    }

    result: dict[str, Any] = {"run_id": run_id, "generated_at": datetime.now().isoformat(), "checks": {}}
    for check_name, sql in sqls.items():
        rows = run_sql(sql)
        result["checks"][check_name] = rows

    revenue_csv = OUT_DIR / f"bi_top_industry_revenue_latest_{run_id}.csv"
    profit_csv = OUT_DIR / f"bi_top5_industry_profit_latest_{run_id}.csv"
    write_csv(revenue_csv, result["checks"]["top_industry_revenue_latest"])
    write_csv(profit_csv, result["checks"]["top5_industry_profit_latest"])

    report_path = OUT_DIR / f"bi_industry_quality_report_{run_id}.json"
    report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report_path)


if __name__ == "__main__":
    main()
