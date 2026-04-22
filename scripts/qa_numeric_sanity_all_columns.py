import csv
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

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

RATIO_COLUMNS = {
    "gross_margin", "operating_margin", "net_margin", "roe", "roa", "debt_to_equity", "current_ratio", "asset_turnover"
}

# In stored unit (bn VND for money-like fields), conservative sanity caps.
AMOUNT_MAX_ABS = 10_000_000.0
EPS_MAX_ABS = 100_000.0
RATIO_MAX_ABS = 100.0


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


def _threshold_for(col: str) -> float:
    if col == "eps":
        return EPS_MAX_ABS
    if col in RATIO_COLUMNS:
        return RATIO_MAX_ABS
    return AMOUNT_MAX_ABS


def main() -> None:
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    rows = _iter_rows()
    df = pd.DataFrame(rows)
    if df.empty:
        report = {"run_id": run_id, "rows": 0, "message": "no data"}
        out_path = out_dir / f"qa_numeric_sanity_all_columns_{run_id}.json"
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(out_path)
        return

    col_summary: list[dict[str, Any]] = []
    suspicious_rows: list[dict[str, Any]] = []

    for col in TARGET_COLUMNS:
        ser = pd.to_numeric(df[col], errors="coerce")
        total = int(len(ser))
        non_null = int(ser.notna().sum())
        null_count = total - non_null
        null_pct = round((null_count / total) * 100.0, 2) if total > 0 else 0.0
        if non_null == 0:
            col_summary.append(
                {
                    "column_name": col,
                    "rows": total,
                    "non_null_count": non_null,
                    "null_pct": null_pct,
                    "max_abs": None,
                    "p99_abs": None,
                    "threshold_abs": _threshold_for(col),
                    "outlier_count": 0,
                    "outlier_pct_non_null": 0.0,
                }
            )
            continue

        abs_ser = ser.abs()
        max_abs = float(abs_ser.max())
        p99_abs = float(abs_ser.quantile(0.99))
        threshold = _threshold_for(col)
        mask_outlier = abs_ser > threshold
        outlier_count = int(mask_outlier.sum())
        outlier_pct_non_null = round((outlier_count / non_null) * 100.0, 2) if non_null > 0 else 0.0
        col_summary.append(
            {
                "column_name": col,
                "rows": total,
                "non_null_count": non_null,
                "null_pct": null_pct,
                "max_abs": round(max_abs, 6),
                "p99_abs": round(p99_abs, 6),
                "threshold_abs": threshold,
                "outlier_count": outlier_count,
                "outlier_pct_non_null": outlier_pct_non_null,
            }
        )

        if outlier_count > 0 and len(suspicious_rows) < 500:
            idxs = df.index[mask_outlier].tolist()[:50]
            for i in idxs:
                v = df.at[i, col]
                if v is None:
                    continue
                try:
                    fv = float(v)
                    if not math.isfinite(fv):
                        continue
                except (TypeError, ValueError):
                    continue
                suspicious_rows.append(
                    {
                        "column_name": col,
                        "ticker": df.at[i, "ticker"],
                        "report_type": df.at[i, "report_type"],
                        "period": df.at[i, "period"],
                        "source": df.at[i, "source"],
                        "value": fv,
                        "abs_value": abs(fv),
                        "threshold_abs": threshold,
                    }
                )

    # Revenue-profit consistency quick checks
    revenue = pd.to_numeric(df["revenue"], errors="coerce")
    pat = pd.to_numeric(df["profit_after_tax"], errors="coerce")
    consistency = {
        "rows_total": int(len(df)),
        "revenue_null_profit_not_null": int(((revenue.isna()) & (pat.notna())).sum()),
        "revenue_not_null_profit_null": int(((revenue.notna()) & (pat.isna())).sum()),
        "revenue_outlier_abs_gt_10m_bn": int((revenue.abs() > AMOUNT_MAX_ABS).sum()),
        "profit_outlier_abs_gt_10m_bn": int((pat.abs() > AMOUNT_MAX_ABS).sum()),
    }

    report = {
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(),
        "rows_total": int(len(df)),
        "sanity_threshold": {
            "amount_max_abs_bn": AMOUNT_MAX_ABS,
            "eps_max_abs": EPS_MAX_ABS,
            "ratio_max_abs": RATIO_MAX_ABS,
        },
        "revenue_profit_consistency": consistency,
        "column_summary": col_summary,
        "suspicious_rows_sample": suspicious_rows,
    }

    json_path = out_dir / f"qa_numeric_sanity_all_columns_{run_id}.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_path = out_dir / f"qa_numeric_sanity_all_columns_{run_id}.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "column_name",
                "rows",
                "non_null_count",
                "null_pct",
                "max_abs",
                "p99_abs",
                "threshold_abs",
                "outlier_count",
                "outlier_pct_non_null",
            ],
        )
        writer.writeheader()
        writer.writerows(col_summary)

    suspicious_csv_path = out_dir / f"qa_numeric_sanity_suspicious_sample_{run_id}.csv"
    with suspicious_csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "column_name",
                "ticker",
                "report_type",
                "period",
                "source",
                "value",
                "abs_value",
                "threshold_abs",
            ],
        )
        writer.writeheader()
        writer.writerows(suspicious_rows)

    print(json_path)
    print(csv_path)
    print(suspicious_csv_path)


if __name__ == "__main__":
    main()
