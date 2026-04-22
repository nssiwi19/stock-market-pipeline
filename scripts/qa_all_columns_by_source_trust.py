import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import get_supabase_client

TARGET_COLUMNS = [
    "revenue", "cogs", "gross_profit", "financial_income", "financial_expense", "interest_expense",
    "selling_expense", "general_admin_expense", "operating_profit", "other_income", "other_expense",
    "profit_before_tax", "profit_after_tax", "parent_profit_after_tax", "minority_profit",
    "depreciation_amortization", "ebit", "ebitda", "eps", "cash_and_cash_equivalents",
    "short_term_investments", "short_term_receivables", "inventory", "other_current_assets",
    "total_current_assets", "long_term_receivables", "fixed_assets", "investment_properties",
    "long_term_assets", "total_assets", "short_term_debt", "accounts_payable", "short_term_liabilities",
    "total_short_term_liabilities", "long_term_debt", "total_long_term_liabilities", "total_liabilities",
    "owner_equity", "equity", "retained_earnings", "share_capital", "total_equity_and_liabilities",
    "cash_flow_operating", "cash_flow_investing", "cash_flow_financing", "net_cash_flow", "capex",
    "gross_margin", "operating_margin", "net_margin", "roe", "roa", "debt_to_equity", "current_ratio",
    "asset_turnover",
]
TRUSTED_SOURCES = {"cafef_requests", "vietstock_financeinfo", "cafef_cloudscraper"}


def _iter_rows() -> list[dict[str, Any]]:
    supabase = get_supabase_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 1000
    select_cols = ",".join(["ticker", "report_type", "period", "source"] + TARGET_COLUMNS)
    while True:
        resp = (
            supabase.table("financial_reports")
            .select(select_cols)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def _source_group(src: str | None) -> str:
    parts = {s.strip() for s in str(src or "").split("+") if s.strip()}
    if not parts:
        return "unknown_source"
    trusted_overlap = bool(parts & TRUSTED_SOURCES)
    if trusted_overlap:
        return "trusted_or_mixed"
    return "untrusted_only"


def main() -> None:
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    rows = _iter_rows()
    group_rows: dict[str, list[dict[str, Any]]] = {
        "trusted_or_mixed": [],
        "untrusted_only": [],
        "unknown_source": [],
    }
    for r in rows:
        group_rows[_source_group(r.get("source"))].append(r)

    by_group_summary: dict[str, dict[str, Any]] = {}
    csv_rows: list[dict[str, Any]] = []
    for group, items in group_rows.items():
        total = len(items)
        null_by_col: dict[str, int] = {c: 0 for c in TARGET_COLUMNS}
        for r in items:
            for c in TARGET_COLUMNS:
                if r.get(c) is None:
                    null_by_col[c] += 1
        fill_rate = 0.0
        if total > 0:
            total_cells = total * len(TARGET_COLUMNS)
            total_null = sum(null_by_col.values())
            fill_rate = round((1.0 - total_null / total_cells) * 100.0, 2)
        by_group_summary[group] = {
            "rows": total,
            "fill_rate_pct": fill_rate,
            "null_by_column": null_by_col,
        }
        for c in TARGET_COLUMNS:
            null_count = null_by_col[c]
            null_pct = round((null_count / total) * 100.0, 2) if total > 0 else 0.0
            csv_rows.append(
                {
                    "group": group,
                    "column_name": c,
                    "rows": total,
                    "null_count": null_count,
                    "null_pct": null_pct,
                }
            )

    report = {
        "run_id": run_id,
        "rows_total": len(rows),
        "trusted_sources": sorted(list(TRUSTED_SOURCES)),
        "group_summary": by_group_summary,
    }

    report_path = out_dir / f"qa_all_columns_by_source_trust_{run_id}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_path = out_dir / f"qa_all_columns_by_source_trust_{run_id}.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["group", "column_name", "rows", "null_count", "null_pct"])
        writer.writeheader()
        writer.writerows(csv_rows)

    print(report_path)
    print(csv_path)


if __name__ == "__main__":
    main()
