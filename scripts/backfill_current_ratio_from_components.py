import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def main() -> None:
    load_dotenv()
    db = os.getenv("SUPABASE_DB_URI") or os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError("Missing SUPABASE_DB_URI or DATABASE_URL")

    conn = psycopg2.connect(db)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("select count(*) from public.financial_reports where current_ratio is not null")
    before_ratio = cur.fetchone()[0]

    cur.execute(
        """
        update public.financial_reports
        set total_current_assets =
            coalesce(
                total_current_assets,
                case
                    when total_assets is not null and long_term_assets is not null then total_assets - long_term_assets
                    else null
                end,
                case
                    when cash_and_cash_equivalents is not null
                      and short_term_investments is not null
                      and short_term_receivables is not null
                      and inventory is not null
                      and other_current_assets is not null
                    then cash_and_cash_equivalents
                       + short_term_investments
                       + short_term_receivables
                       + inventory
                       + other_current_assets
                    else null
                end
            ),
            total_short_term_liabilities =
            coalesce(
                total_short_term_liabilities,
                short_term_liabilities,
                case
                    when total_liabilities is not null and total_long_term_liabilities is not null
                    then total_liabilities - total_long_term_liabilities
                    else null
                end,
                case
                    when short_term_debt is not null and accounts_payable is not null
                    then short_term_debt + accounts_payable
                    else null
                end
            )
        where current_ratio is null
        """
    )
    rows_updated_components = cur.rowcount

    cur.execute(
        """
        update public.financial_reports
        set current_ratio =
            case
                when total_current_assets is not null
                 and total_short_term_liabilities is not null
                 and total_short_term_liabilities <> 0
                then total_current_assets / total_short_term_liabilities
                else current_ratio
            end
        where current_ratio is null
          and total_current_assets is not null
          and total_short_term_liabilities is not null
          and total_short_term_liabilities <> 0
        """
    )
    rows_updated_ratio = cur.rowcount

    # Sanity clamp for ratio field.
    cur.execute(
        """
        update public.financial_reports
        set current_ratio = null
        where current_ratio is not null
          and abs(current_ratio) > 100
        """
    )
    rows_clamped = cur.rowcount

    cur.execute("select count(*) from public.financial_reports where current_ratio is not null")
    after_ratio = cur.fetchone()[0]

    report = {
        "before_current_ratio_non_null": before_ratio,
        "rows_updated_components": rows_updated_components,
        "rows_updated_current_ratio": rows_updated_ratio,
        "rows_clamped_outlier_ratio": rows_clamped,
        "after_current_ratio_non_null": after_ratio,
    }

    out_path = Path("scripts/output/backfill_current_ratio_from_components_report.json")
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_path)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

