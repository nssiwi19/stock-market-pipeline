import json
import math
import sys
import time
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
RATIO_COLUMNS = {
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roa",
    "debt_to_equity",
    "current_ratio",
    "asset_turnover",
}
TRUSTED_SOURCES = {"cafef_requests", "vietstock_financeinfo", "cafef_cloudscraper"}
MONEY_COLUMNS = [c for c in TARGET_COLUMNS if c not in RATIO_COLUMNS and c != "eps"]

AMOUNT_MAX_ABS = 10_000_000.0
EPS_MAX_ABS = 100_000.0
RATIO_MAX_ABS = 100.0
API_RETRY_ATTEMPTS = 5
# Detect rows likely stored in "trieu dong" while DB expects "ty dong".
# We use revenue/profit anchors (not assets) to avoid false positives on large banks.
SCALE_ANCHOR_COLUMNS = [
    "revenue",
    "cogs",
    "gross_profit",
    "operating_profit",
    "profit_before_tax",
    "profit_after_tax",
    "cash_flow_operating",
]
SCALE_TRIGGER_ABS = 800_000.0
SCALE_DIVISOR = 1000.0


def _execute_with_retry(action):
    last_exc: Exception | None = None
    for i in range(1, API_RETRY_ATTEMPTS + 1):
        try:
            return action()
        except Exception as exc:
            last_exc = exc
            if i >= API_RETRY_ATTEMPTS:
                raise
            time.sleep(1.2 * i)
    if last_exc:
        raise last_exc
    raise RuntimeError("Unexpected retry state")


def _iter_rows() -> list[dict[str, Any]]:
    supabase = get_supabase_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 1000
    select_cols = ",".join(["ticker", "report_type", "period", "source"] + TARGET_COLUMNS)
    while True:
        resp = _execute_with_retry(
            lambda: supabase.table("financial_reports")
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


def _to_num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _threshold(col: str) -> float:
    if col == "eps":
        return EPS_MAX_ABS
    if col in RATIO_COLUMNS:
        return RATIO_MAX_ABS
    return AMOUNT_MAX_ABS


def _normalize_row_scale_if_needed(rec: dict[str, Any], src_parts: set[str]) -> bool:
    """
    Normalize likely unit drift rows (mostly from vietstock_financeinfo).
    Returns True if row was scaled.
    """
    if "vietstock_financeinfo" not in src_parts:
        return False
    anchors: list[float] = []
    for col in SCALE_ANCHOR_COLUMNS:
        v = _to_num(rec.get(col))
        if v is not None:
            anchors.append(abs(v))
    if not anchors:
        return False
    if max(anchors) <= SCALE_TRIGGER_ABS:
        return False
    for col in MONEY_COLUMNS:
        v = _to_num(rec.get(col))
        if v is None:
            rec[col] = None
            continue
        rec[col] = v / SCALE_DIVISOR
    return True


def _recompute_ratios(rec: dict[str, Any]) -> None:
    revenue = _to_num(rec.get("revenue"))
    gross_profit = _to_num(rec.get("gross_profit"))
    operating_profit = _to_num(rec.get("operating_profit"))
    pat = _to_num(rec.get("profit_after_tax"))
    equity = _to_num(rec.get("equity"))
    total_assets = _to_num(rec.get("total_assets"))
    total_liabilities = _to_num(rec.get("total_liabilities"))
    tca = _to_num(rec.get("total_current_assets"))
    tstl = _to_num(rec.get("total_short_term_liabilities"))

    rec["gross_margin"] = (gross_profit / revenue * 100.0) if revenue not in (None, 0.0) and gross_profit is not None else None
    rec["operating_margin"] = (
        operating_profit / revenue * 100.0 if revenue not in (None, 0.0) and operating_profit is not None else None
    )
    rec["net_margin"] = (pat / revenue * 100.0) if revenue not in (None, 0.0) and pat is not None else None
    rec["roe"] = (pat / equity * 100.0) if equity not in (None, 0.0) and pat is not None else None
    rec["roa"] = (pat / total_assets * 100.0) if total_assets not in (None, 0.0) and pat is not None else None
    rec["debt_to_equity"] = (
        total_liabilities / equity if total_liabilities is not None and equity not in (None, 0.0) else None
    )
    rec["current_ratio"] = tca / tstl if tca is not None and tstl not in (None, 0.0) else None
    rec["asset_turnover"] = revenue / total_assets if revenue is not None and total_assets not in (None, 0.0) else None


def main() -> None:
    supabase = get_supabase_client()
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    rows = _iter_rows()

    to_delete: list[dict[str, Any]] = []
    to_update: list[dict[str, Any]] = []
    stat = {
        "rows_scanned": len(rows),
        "rows_deleted_ocr_or_untrusted": 0,
        "rows_scaled_unit_fix": 0,
        "cells_nullified_outlier_or_invalid": 0,
        "rows_recomputed_ratios": 0,
        "rows_updated": 0,
    }
    sample_deleted: list[dict[str, Any]] = []
    sample_changed: list[dict[str, Any]] = []

    for row in rows:
        src_raw = str(row.get("source") or "")
        src_parts = {s.strip() for s in src_raw.split("+") if s.strip()}

        # Hard rule: remove OCR/document-only or any row with OCR source tag.
        if "vietstock_bctc_documents" in src_parts:
            to_delete.append(
                {
                    "ticker": row.get("ticker"),
                    "report_type": row.get("report_type"),
                    "period": row.get("period"),
                    "source": row.get("source"),
                }
            )
            if len(sample_deleted) < 100:
                sample_deleted.append(to_delete[-1])
            continue
        if src_parts and src_parts.isdisjoint(TRUSTED_SOURCES):
            to_delete.append(
                {
                    "ticker": row.get("ticker"),
                    "report_type": row.get("report_type"),
                    "period": row.get("period"),
                    "source": row.get("source"),
                }
            )
            if len(sample_deleted) < 100:
                sample_deleted.append(to_delete[-1])
            continue

        changed = False
        update_rec: dict[str, Any] = {
            "ticker": row.get("ticker"),
            "report_type": row.get("report_type"),
            "period": row.get("period"),
        }

        # 1) normalize + null invalid/outlier raw cells
        for col in TARGET_COLUMNS:
            v = row.get(col)
            if v is None:
                update_rec[col] = None
                continue
            num = _to_num(v)
            if num is None:
                update_rec[col] = None
                stat["cells_nullified_outlier_or_invalid"] += 1
                changed = True
                continue
            if abs(num) > _threshold(col):
                update_rec[col] = None
                stat["cells_nullified_outlier_or_invalid"] += 1
                changed = True
                continue
            update_rec[col] = num

        # 1.5) scale normalization for likely unit drift rows
        if _normalize_row_scale_if_needed(update_rec, src_parts):
            stat["rows_scaled_unit_fix"] += 1
            changed = True

        # 2) recompute ratio columns from cleaned base metrics
        before_ratios = {c: update_rec.get(c) for c in RATIO_COLUMNS}
        _recompute_ratios(update_rec)
        after_ratios = {c: update_rec.get(c) for c in RATIO_COLUMNS}
        if before_ratios != after_ratios:
            changed = True
            stat["rows_recomputed_ratios"] += 1

        # 3) enforce ratio caps after recompute
        for col in RATIO_COLUMNS:
            rv = _to_num(update_rec.get(col))
            if rv is None:
                update_rec[col] = None
                continue
            if abs(rv) > RATIO_MAX_ABS:
                update_rec[col] = None
                stat["cells_nullified_outlier_or_invalid"] += 1
                changed = True

        if changed:
            to_update.append(update_rec)
            if len(sample_changed) < 120:
                sample_changed.append(
                    {
                        "ticker": update_rec["ticker"],
                        "report_type": update_rec["report_type"],
                        "period": update_rec["period"],
                    }
                )

    # Apply deletes first.
    for d in to_delete:
        _execute_with_retry(
            lambda dd=d: supabase.table("financial_reports")
            .delete()
            .eq("ticker", dd["ticker"])
            .eq("report_type", dd["report_type"])
            .eq("period", dd["period"])
            .execute()
        )
    stat["rows_deleted_ocr_or_untrusted"] = len(to_delete)

    # Apply updates in batches.
    batch_size = 200
    updated = 0
    for i in range(0, len(to_update), batch_size):
        batch = to_update[i : i + batch_size]
        _execute_with_retry(
            lambda b=batch: supabase.table("financial_reports")
            .upsert(b, on_conflict="ticker,report_type,period")
            .execute()
        )
        updated += len(batch)
    stat["rows_updated"] = updated

    report = {
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(),
        "thresholds": {
            "amount_max_abs_bn": AMOUNT_MAX_ABS,
            "eps_max_abs": EPS_MAX_ABS,
            "ratio_max_abs": RATIO_MAX_ABS,
        },
        "trusted_sources": sorted(list(TRUSTED_SOURCES)),
        "stats": stat,
        "sample_deleted": sample_deleted,
        "sample_changed": sample_changed,
    }
    report_path = out_dir / f"strict_clean_financial_reports_{run_id}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report_path)


if __name__ == "__main__":
    main()
