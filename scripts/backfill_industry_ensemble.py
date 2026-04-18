"""
Ensemble industry inference for `tickers`:
1) Text model from company_name (reuse existing hybrid text predictor)
2) Financial-signature model from financial_reports (industry centroid similarity)

Write strategy:
- Always write `industry_inferred*` when supported
- Write back to `industry` only when confidence >= threshold
"""

from __future__ import annotations

import csv
import json
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
DEFAULT_METHOD_THRESHOLDS = {
    "domain_rule": 0.92,
    "rule_keyword": 0.90,
    "ens_agree": 0.80,
    "tfidf_centroid": 0.86,
    "fin_only": 0.84,
    "ens_conflict_text": 0.88,
    "ens_conflict_fin": 0.90,
    "text_only": 0.92,
}
DEFAULT_BLOCKED_METHODS = {"ens_conflict_fin", "text_only"}


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


def _normalize_text(text: str) -> str:
    return text_inf._normalize_text(text)


def _build_default_domain_rules() -> list[dict]:
    return [
        {"target_contains": "ngan hang", "keywords": ["ngan hang", "bank"]},
        {"target_contains": "chung khoan", "keywords": ["chung khoan", "securities"]},
        {"target_contains": "bao hiem", "keywords": ["bao hiem", "insurance"]},
        {"target_contains": "bat dong san", "keywords": ["bat dong san", "real estate"]},
        {"target_contains": "dien", "keywords": ["dien luc", "thuy dien", "nhiet dien", "power"]},
        {"target_contains": "dau khi", "keywords": ["dau khi", "petro", "gas"]},
        {"target_contains": "thep", "keywords": ["thep", "steel"]},
        {"target_contains": "duoc", "keywords": ["duoc", "pharma", "pharmaceutical"]},
        {"target_contains": "thuy san", "keywords": ["thuy san", "thuysan", "seafood"]},
        {"target_contains": "det may", "keywords": ["det may", "textile", "garment"]},
        {"target_contains": "van tai", "keywords": ["van tai", "logistics", "transport"]},
        {"target_contains": "hang khong", "keywords": ["hang khong", "aviation", "airline"]},
        {"target_contains": "xi mang", "keywords": ["xi mang", "cement"]},
        {"target_contains": "phan bon", "keywords": ["phan bon", "fertilizer"]},
    ]


def _load_domain_rules() -> list[dict]:
    rules_path = os.getenv("INFER_DOMAIN_RULES_PATH", "").strip()
    if not rules_path:
        return _build_default_domain_rules()
    path = Path(rules_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / rules_path
    if not path.exists():
        return _build_default_domain_rules()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    except Exception:
        pass
    return _build_default_domain_rules()


def _resolve_rule_targets(known_labels: List[str], domain_rules: list[dict]) -> Dict[str, str]:
    label_pairs = [(label, _normalize_text(label)) for label in known_labels]
    resolved = {}
    for idx, rule in enumerate(domain_rules):
        target_contains = _normalize_text(rule.get("target_contains", ""))
        if not target_contains:
            continue
        candidates = [label for label, norm in label_pairs if target_contains in norm]
        if len(candidates) == 1:
            resolved[str(idx)] = candidates[0]
    return resolved


def _infer_domain_rule(company_name: str, domain_rules: list[dict], resolved_targets: Dict[str, str]) -> Tuple[str | None, float, str]:
    name_norm = _normalize_text(company_name or "")
    if not name_norm:
        return None, 0.0, "none"

    matched_labels = []
    for idx, rule in enumerate(domain_rules):
        label = resolved_targets.get(str(idx))
        if not label:
            continue
        keywords = [_normalize_text(k) for k in rule.get("keywords", []) if str(k).strip()]
        for kw in keywords:
            if kw and kw in name_norm:
                matched_labels.append(label)
                break
    matched_labels = sorted(set(matched_labels))
    if len(matched_labels) == 1:
        return matched_labels[0], 0.97, "domain_rule"
    return None, 0.0, "none"


def _export_manual_review_csv(rows: List[dict]) -> str:
    if not rows:
        return ""
    raw_path = os.getenv("INFER_REVIEW_EXPORT_PATH", "scripts/output/industry_manual_review.csv").strip()
    path = Path(raw_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ticker",
        "company_name",
        "exchange",
        "industry",
        "industry_inferred",
        "industry_inferred_confidence",
        "industry_inferred_method",
        "reason",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})
    return str(path)


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
    domain_pred: Tuple[str | None, float, str],
    text_pred: Tuple[str | None, float, str],
    fin_pred: Tuple[str | None, float, str],
) -> Tuple[str | None, float, str]:
    d_label, d_conf, d_method = domain_pred
    t_label, t_conf, t_method = text_pred
    f_label, f_conf, f_method = fin_pred

    if d_label:
        return d_label, d_conf, d_method
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


def _get_method_thresholds(default_threshold: float) -> Dict[str, float]:
    thresholds = dict(DEFAULT_METHOD_THRESHOLDS)
    # Optional override via env, format:
    # rule_keyword:0.9,ens_agree:0.8,tfidf_centroid:0.86,fin_only:0.84
    raw = os.getenv("INFER_ENSEMBLE_METHOD_THRESHOLDS", "").strip()
    if raw:
        for pair in raw.split(","):
            if ":" not in pair:
                continue
            method, val = pair.split(":", 1)
            method = method.strip()
            try:
                thresholds[method] = float(val.strip())
            except ValueError:
                continue
    # Any method not in dict falls back to global threshold
    for method in ("domain_rule", "rule_keyword", "ens_agree", "tfidf_centroid", "fin_only", "ens_conflict_text", "ens_conflict_fin", "text_only"):
        thresholds.setdefault(method, default_threshold)
    return thresholds


def _get_blocked_methods() -> set[str]:
    raw = os.getenv("INFER_ENSEMBLE_BLOCK_METHODS", "").strip()
    if not raw:
        return set(DEFAULT_BLOCKED_METHODS)
    return {m.strip() for m in raw.split(",") if m.strip()}


def _allow_writeback(
    method: str,
    confidence: float,
    global_min_confidence: float,
    method_thresholds: Dict[str, float],
    blocked_methods: set[str],
) -> bool:
    if method in blocked_methods:
        return False
    min_conf = max(global_min_confidence, method_thresholds.get(method, global_min_confidence))
    return confidence >= min_conf


def infer_and_backfill_ensemble():
    supabase = get_supabase_client()
    min_confidence = float(os.getenv("INFER_ENSEMBLE_MIN_CONFIDENCE", "0.78"))
    writeback_industry = os.getenv("INFER_WRITEBACK_INDUSTRY", "true").strip().lower() in {
        "1", "true", "yes", "on"
    }
    dry_run = os.getenv("INFER_DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "on"}
    method_thresholds = _get_method_thresholds(min_confidence)
    blocked_methods = _get_blocked_methods()

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
    domain_rules = _load_domain_rules()
    resolved_rule_targets = _resolve_rule_targets(labels, domain_rules)

    fin_rows = _fetch_financial_rows(supabase)
    ticker_fin_vec_raw = _aggregate_ticker_financial_vectors(fin_rows)
    fin_model = _build_financial_model(labeled_rows, ticker_fin_vec_raw)

    now_iso = datetime.now(timezone.utc).isoformat()
    updates = []
    accepted = 0
    agreed = 0
    used_financial = 0
    method_counts = Counter()
    manual_review_rows = []
    for row in missing_rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        domain_pred = _infer_domain_rule(row.get("company_name") or "", domain_rules, resolved_rule_targets)
        text_pred = text_inf._infer_single(row, text_model, keyword_targets)
        fin_pred = _infer_financial_industry(ticker, fin_model) if fin_model else (None, 0.0, "none")
        label, confidence, method = _combine_predictions(domain_pred, text_pred, fin_pred)
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
        can_write = _allow_writeback(
            method=method,
            confidence=confidence,
            global_min_confidence=min_confidence,
            method_thresholds=method_thresholds,
            blocked_methods=blocked_methods,
        )
        if writeback_industry and can_write:
            payload["industry"] = label
            accepted += 1
        else:
            reason = "blocked_method" if method in blocked_methods else "below_threshold"
            manual_review_rows.append(
                {
                    "ticker": ticker,
                    "company_name": row.get("company_name"),
                    "exchange": row.get("exchange"),
                    "industry": row.get("industry"),
                    "industry_inferred": label,
                    "industry_inferred_confidence": confidence,
                    "industry_inferred_method": method,
                    "reason": reason,
                }
            )
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
    review_path = _export_manual_review_csv(manual_review_rows)

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
        "method_thresholds": method_thresholds,
        "blocked_methods": sorted(blocked_methods),
        "manual_review_rows": len(manual_review_rows),
        "manual_review_export_path": review_path,
        "writeback_industry": writeback_industry,
        "dry_run": dry_run,
        "fin_min_features": max(int(os.getenv("INFER_FIN_MIN_FEATURES", "2")), 1),
        "supports_inferred_columns": supports_inferred_columns,
    }


if __name__ == "__main__":
    print(infer_and_backfill_ensemble())
