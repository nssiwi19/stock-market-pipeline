"""
Ensemble industry inference for `tickers`:
1) Text model from company_name (reuse existing hybrid text predictor)
2) Financial-signature model from financial_reports (industry centroid similarity)

Write strategy:
- Always write `industry_inferred*` when supported
- Write back to `industry` only when confidence >= threshold
"""

from __future__ import annotations

import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import get_supabase_client
from scripts import backfill_industry_inference as text_inf


FIN_FEATURES = [
    "revenue",
    "profit_after_tax",
    "total_assets",
    "total_liabilities",
    "owner_equity",
    "cash_flow_operating",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roa",
    "debt_to_equity",
    "current_ratio",
    "asset_turnover",
    "inventory",
    "cogs",
]
LOG_SCALE_FEATURES = {
    "revenue",
    "profit_after_tax",
    "total_assets",
    "total_liabilities",
    "owner_equity",
    "cash_flow_operating",
    "inventory",
    "cogs",
}


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _signed_log1p(value: float) -> float:
    if value == 0:
        return 0.0
    sign = 1.0 if value > 0 else -1.0
    return sign * math.log1p(abs(value))


def _fetch_financial_rows(supabase, page_size: int = 1000) -> List[dict]:
    cols = "ticker," + ",".join(FIN_FEATURES)
    rows = []
    offset = 0
    while True:
        batch = (
            supabase.table("financial_reports")
            .select(cols)
            .range(offset, offset + page_size - 1)
            .execute()
            .data
            or []
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def _aggregate_ticker_financial_vectors(fin_rows: List[dict]) -> Dict[str, Dict[str, float]]:
    sums = defaultdict(lambda: Counter())
    counts = defaultdict(lambda: Counter())
    for row in fin_rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        for feature in FIN_FEATURES:
            val = _to_float(row.get(feature))
            if val is None:
                continue
            sums[ticker][feature] += val
            counts[ticker][feature] += 1

    vectors = {}
    for ticker, feature_sums in sums.items():
        vec = {}
        for feature, total in feature_sums.items():
            cnt = counts[ticker][feature]
            if cnt > 0:
                mean_val = total / cnt
                # Monetary features vary by many orders of magnitude; log-scaling improves similarity stability.
                if feature in LOG_SCALE_FEATURES:
                    mean_val = _signed_log1p(mean_val)
                vec[feature] = mean_val
        if vec:
            vectors[ticker] = vec
    return vectors


def _compute_standardization(vectors: Dict[str, Dict[str, float]]) -> Tuple[Dict[str, float], Dict[str, float]]:
    values = defaultdict(list)
    for vec in vectors.values():
        for feature, val in vec.items():
            values[feature].append(val)
    means = {}
    stds = {}
    for feature, arr in values.items():
        if not arr:
            continue
        mean = sum(arr) / len(arr)
        var = sum((x - mean) ** 2 for x in arr) / len(arr)
        means[feature] = mean
        stds[feature] = math.sqrt(var) or 1.0
    return means, stds


def _standardize_vec(vec: Dict[str, float], means: Dict[str, float], stds: Dict[str, float]) -> Dict[str, float]:
    out = {}
    for feature, val in vec.items():
        if feature in means and feature in stds:
            out[feature] = (val - means[feature]) / stds[feature]
    return out


def _cosine(v1: Dict[str, float], v2: Dict[str, float]) -> float:
    if not v1 or not v2:
        return 0.0
    dot = 0.0
    for k, v in v1.items():
        dot += v * v2.get(k, 0.0)
    n1 = math.sqrt(sum(v * v for v in v1.values())) or 1.0
    n2 = math.sqrt(sum(v * v for v in v2.values())) or 1.0
    return dot / (n1 * n2)


def _build_financial_model(
    labeled_rows: List[dict],
    ticker_fin_vec_raw: Dict[str, Dict[str, float]],
):
    train_vectors = {}
    for row in labeled_rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        vec = ticker_fin_vec_raw.get(ticker)
        if vec:
            train_vectors[ticker] = vec
    if not train_vectors:
        return None

    means, stds = _compute_standardization(train_vectors)
    ticker_vec = {
        ticker: _standardize_vec(vec, means, stds)
        for ticker, vec in train_vectors.items()
    }

    industry_centroids = defaultdict(lambda: Counter())
    industry_counts = Counter()
    for row in labeled_rows:
        ticker = row.get("ticker")
        industry = text_inf._clean_nullable_text(row.get("industry"))
        if not ticker or not industry:
            continue
        vec = ticker_vec.get(ticker)
        if not vec:
            continue
        for k, v in vec.items():
            industry_centroids[industry][k] += v
        industry_counts[industry] += 1

    centroids = {}
    for industry, counter in industry_centroids.items():
        cnt = max(industry_counts[industry], 1)
        centroids[industry] = {k: v / cnt for k, v in counter.items()}
    if not centroids:
        return None

    return {
        "means": means,
        "stds": stds,
        "centroids": centroids,
        "ticker_fin_vec_raw": ticker_fin_vec_raw,
    }


def _infer_financial_industry(ticker: str, fin_model: dict) -> Tuple[str | None, float, str]:
    raw_vec = fin_model["ticker_fin_vec_raw"].get(ticker)
    if not raw_vec:
        return None, 0.0, "none"
    vec = _standardize_vec(raw_vec, fin_model["means"], fin_model["stds"])
    min_features = max(int(os.getenv("INFER_FIN_MIN_FEATURES", "2")), 1)
    if len(vec) < min_features:
        return None, 0.0, "none"

    scores = []
    for industry, centroid in fin_model["centroids"].items():
        sim = _cosine(vec, centroid)
        scores.append((industry, sim))
    scores.sort(key=lambda x: x[1], reverse=True)
    if not scores:
        return None, 0.0, "none"

    best_label, best_score = scores[0]
    second_score = scores[1][1] if len(scores) > 1 else 0.0
    margin = max(best_score - second_score, 0.0)
    confidence = min(0.52 + margin * 1.8 + max(best_score, 0.0) * 0.35, 0.93)
    return best_label, round(confidence, 4), "financial_centroid"


def _combine_predictions(
    text_pred: Tuple[str | None, float, str],
    fin_pred: Tuple[str | None, float, str],
) -> Tuple[str | None, float, str]:
    t_label, t_conf, t_method = text_pred
    f_label, f_conf, f_method = fin_pred

    if t_label and f_label:
        if t_label == f_label:
            conf = min(max(t_conf, f_conf) + 0.06, 0.98)
            return t_label, round(conf, 4), "ens_agree"
        if t_conf >= f_conf:
            return t_label, round(max(t_conf * 0.88, 0.0), 4), "ens_conflict_text"
        return f_label, round(max(f_conf * 0.88, 0.0), 4), "ens_conflict_fin"
    if t_label:
        return t_label, t_conf, "text_only"
    if f_label:
        return f_label, f_conf, "fin_only"
    return None, 0.0, "none"


def infer_and_backfill_ensemble():
    supabase = get_supabase_client()
    min_confidence = float(os.getenv("INFER_ENSEMBLE_MIN_CONFIDENCE", "0.78"))
    writeback_industry = os.getenv("INFER_WRITEBACK_INDUSTRY", "true").strip().lower() in {
        "1", "true", "yes", "on"
    }
    dry_run = os.getenv("INFER_DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "on"}

    supports_inferred_columns = True
    try:
        all_rows = text_inf._fetch_all_tickers(
            supabase,
            select_cols="ticker,company_name,industry,industry_inferred,industry_inferred_confidence,exchange",
        )
    except Exception:
        supports_inferred_columns = False
        all_rows = text_inf._fetch_all_tickers(supabase, select_cols="ticker,company_name,industry,exchange")

    labeled_rows = [r for r in all_rows if not text_inf._is_missing_industry(r.get("industry"))]
    missing_rows = [r for r in all_rows if text_inf._is_missing_industry(r.get("industry"))]
    if not labeled_rows or not missing_rows:
        return {
            "success": True,
            "message": "No labeled rows or no missing rows to process.",
            "labeled_rows": len(labeled_rows),
            "missing_rows": len(missing_rows),
        }

    text_model = text_inf._build_tfidf_model(labeled_rows)
    if not text_model:
        return {"success": False, "message": "Failed to build text model."}
    labels = sorted(
        {
            text_inf._clean_nullable_text(r.get("industry"))
            for r in labeled_rows
            if text_inf._clean_nullable_text(r.get("industry"))
        }
    )
    keyword_targets = text_inf._build_industry_keyword_targets(labels)

    fin_rows = _fetch_financial_rows(supabase)
    ticker_fin_vec_raw = _aggregate_ticker_financial_vectors(fin_rows)
    fin_model = _build_financial_model(labeled_rows, ticker_fin_vec_raw)

    now_iso = datetime.now(timezone.utc).isoformat()
    updates = []
    accepted = 0
    agreed = 0
    used_financial = 0
    method_counts = Counter()
    for row in missing_rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        text_pred = text_inf._infer_single(row, text_model, keyword_targets)
        fin_pred = _infer_financial_industry(ticker, fin_model) if fin_model else (None, 0.0, "none")
        label, confidence, method = _combine_predictions(text_pred, fin_pred)
        if not label:
            continue
        method_counts[method] += 1
        if method in {"fin_only", "ens_conflict_fin", "ens_agree"}:
            used_financial += 1
        if method == "ens_agree":
            agreed += 1

        payload = {
            "ticker": ticker,
            "exchange": text_inf._clean_nullable_text(row.get("exchange")) or "UNKNOWN",
        }
        if supports_inferred_columns:
            payload.update(
                {
                    "industry_inferred": label,
                    "industry_inferred_confidence": confidence,
                    "industry_inferred_method": method,
                    "industry_inferred_at": now_iso,
                }
            )
        if writeback_industry and confidence >= min_confidence:
            payload["industry"] = label
            accepted += 1
        updates.append(payload)

    if not updates:
        return {
            "success": True,
            "message": "No inferable rows.",
            "missing_rows": len(missing_rows),
            "updated_rows": 0,
        }

    batch_size = 200
    updated = 0
    if not dry_run:
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            supabase.table("tickers").upsert(batch, on_conflict="ticker").execute()
            updated += len(batch)

    return {
        "success": True,
        "total_rows": len(all_rows),
        "labeled_rows": len(labeled_rows),
        "missing_rows": len(missing_rows),
        "financial_rows": len(fin_rows),
        "tickers_with_financial_vector": len(ticker_fin_vec_raw),
        "updated_rows": updated if not dry_run else 0,
        "predicted_rows": len(updates),
        "accepted_writeback_rows": accepted,
        "ensemble_agreed_rows": agreed,
        "rows_used_financial_signal": used_financial,
        "method_counts": dict(method_counts),
        "min_confidence": min_confidence,
        "writeback_industry": writeback_industry,
        "dry_run": dry_run,
        "fin_min_features": max(int(os.getenv("INFER_FIN_MIN_FEATURES", "2")), 1),
        "supports_inferred_columns": supports_inferred_columns,
    }


if __name__ == "__main__":
    print(infer_and_backfill_ensemble())
