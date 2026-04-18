"""
Infer missing `industry` in `tickers` from company_name using a hybrid approach:
1) Rule-based keyword match (high confidence)
2) TF-IDF cosine similarity to labeled company names

Safe defaults:
- Always store predictions in `industry_inferred*` columns
- Write back to `industry` only when confidence >= threshold
"""

from __future__ import annotations

import math
import os
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import get_supabase_client


NULL_LIKE = {"", "nan", "none", "null", "<na>", "n/a", "unknown"}
STOPWORDS = {
    "cong", "ty", "co", "phan", "cp", "tnhh", "mot", "thanh", "vien", "tap",
    "doan", "chi", "nhanh", "dau", "tu", "va", "xay", "dung", "thuong", "mai",
    "dich", "vu", "san", "xuat", "quan", "ly", "tong", "ctcp", "jsc", "co.,",
    "ltd", "company",
}
RULE_KEYWORDS = [
    "ngan hang",
    "chung khoan",
    "bao hiem",
    "bat dong san",
    "dien",
    "thep",
    "thuysan",
    "thuy san",
    "det may",
    "van tai",
    "hang khong",
    "duoc",
    "y te",
    "dau khi",
    "phan bon",
    "xi mang",
    "nhua",
]


def _clean_nullable_text(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in NULL_LIKE:
        return None
    return text


def _normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def _tokenize_company_name(name: str) -> List[str]:
    norm = _normalize_text(name)
    tokens = [tok for tok in norm.split() if len(tok) > 1 and tok not in STOPWORDS]
    return tokens


def _is_missing_industry(value) -> bool:
    text = _clean_nullable_text(value)
    return text is None


def _fetch_all_tickers(supabase, page_size: int = 1000, select_cols: str = "ticker,company_name,industry,exchange") -> List[dict]:
    rows = []
    offset = 0
    while True:
        batch = (
            supabase.table("tickers")
            .select(select_cols)
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


def _build_industry_keyword_targets(labels: Iterable[str]) -> Dict[str, str]:
    label_pairs = [(label, _normalize_text(label)) for label in labels]
    matched = {}
    for kw in RULE_KEYWORDS:
        kw_norm = _normalize_text(kw)
        candidates = [label for label, norm in label_pairs if kw_norm in norm]
        if len(candidates) == 1:
            matched[kw_norm] = candidates[0]
    return matched


def _build_tfidf_model(train_rows: List[dict]):
    docs = []
    labels = []
    for row in train_rows:
        name = _clean_nullable_text(row.get("company_name"))
        industry = _clean_nullable_text(row.get("industry"))
        if not name or not industry:
            continue
        tokens = _tokenize_company_name(name)
        if not tokens:
            continue
        docs.append(tokens)
        labels.append(industry)

    if not docs:
        return None

    doc_count = len(docs)
    df = Counter()
    for tokens in docs:
        for tok in set(tokens):
            df[tok] += 1

    idf = {tok: math.log((1 + doc_count) / (1 + c)) + 1.0 for tok, c in df.items()}

    industry_vectors = defaultdict(Counter)
    industry_doc_counts = Counter()
    for tokens, label in zip(docs, labels):
        tf = Counter(tokens)
        vec = Counter()
        for tok, tf_count in tf.items():
            vec[tok] = tf_count * idf.get(tok, 1.0)
        norm = math.sqrt(sum(v * v for v in vec.values())) or 1.0
        for tok, val in vec.items():
            industry_vectors[label][tok] += val / norm
        industry_doc_counts[label] += 1

    for label, vec in industry_vectors.items():
        count = max(industry_doc_counts[label], 1)
        for tok in list(vec.keys()):
            vec[tok] = vec[tok] / count

    priors = {
        label: industry_doc_counts[label] / doc_count
        for label in industry_doc_counts
    }
    return {"idf": idf, "industry_vectors": dict(industry_vectors), "priors": priors}


def _vectorize_tokens(tokens: List[str], idf: Dict[str, float]) -> Counter:
    tf = Counter(tokens)
    vec = Counter()
    for tok, tf_count in tf.items():
        if tok in idf:
            vec[tok] = tf_count * idf[tok]
    norm = math.sqrt(sum(v * v for v in vec.values())) or 1.0
    for tok in list(vec.keys()):
        vec[tok] = vec[tok] / norm
    return vec


def _cosine_similarity(v1: Counter, v2: Counter) -> float:
    if not v1 or not v2:
        return 0.0
    if len(v1) > len(v2):
        v1, v2 = v2, v1
    return sum(v * v2.get(tok, 0.0) for tok, v in v1.items())


def _infer_single(
    row: dict,
    model: dict,
    keyword_targets: Dict[str, str],
) -> Tuple[str | None, float, str]:
    name = _clean_nullable_text(row.get("company_name")) or ""
    name_norm = _normalize_text(name)
    if not name_norm:
        return None, 0.0, "none"

    # 1) Rule-based (high confidence)
    for kw_norm, industry_label in keyword_targets.items():
        if kw_norm in name_norm:
            return industry_label, 0.95, "rule_keyword"

    # 2) TF-IDF centroid similarity
    tokens = _tokenize_company_name(name)
    vec = _vectorize_tokens(tokens, model["idf"])
    if not vec:
        return None, 0.0, "none"

    scored = []
    for label, centroid in model["industry_vectors"].items():
        sim = _cosine_similarity(vec, centroid)
        prior = model["priors"].get(label, 0.0)
        score = sim * 0.9 + prior * 0.1
        scored.append((label, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    if not scored:
        return None, 0.0, "none"
    best_label, best_score = scored[0]
    second_score = scored[1][1] if len(scored) > 1 else 0.0

    margin = max(best_score - second_score, 0.0)
    confidence = min(0.55 + margin * 2.2 + best_score * 0.4, 0.94)
    return best_label, round(confidence, 4), "tfidf_centroid"


def infer_and_backfill():
    supabase = get_supabase_client()
    min_confidence = float(os.getenv("INFER_MIN_CONFIDENCE", "0.75"))
    writeback_industry = os.getenv("INFER_WRITEBACK_INDUSTRY", "true").strip().lower() in {
        "1", "true", "yes", "on"
    }

    supports_inferred_columns = True
    try:
        all_rows = _fetch_all_tickers(
            supabase,
            select_cols="ticker,company_name,industry,industry_inferred,industry_inferred_confidence,exchange",
        )
    except Exception:
        supports_inferred_columns = False
        all_rows = _fetch_all_tickers(supabase, select_cols="ticker,company_name,industry,exchange")
    labeled_rows = [r for r in all_rows if not _is_missing_industry(r.get("industry"))]
    missing_rows = [r for r in all_rows if _is_missing_industry(r.get("industry"))]

    if not labeled_rows:
        return {"success": False, "message": "No labeled rows to train inference model."}
    if not missing_rows:
        return {"success": True, "message": "No missing industry rows.", "updated": 0}

    model = _build_tfidf_model(labeled_rows)
    if not model:
        return {"success": False, "message": "Failed to build model from labeled rows."}

    known_labels = sorted({_clean_nullable_text(r.get("industry")) for r in labeled_rows if _clean_nullable_text(r.get("industry"))})
    keyword_targets = _build_industry_keyword_targets(known_labels)

    now_iso = datetime.now(timezone.utc).isoformat()
    updates = []
    accepted = 0
    for row in missing_rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        inferred, confidence, method = _infer_single(row, model, keyword_targets)
        if not inferred:
            continue
        payload = {
            "ticker": ticker,
            "exchange": _clean_nullable_text(row.get("exchange")) or "UNKNOWN",
        }
        if supports_inferred_columns:
            payload.update(
                {
                    "industry_inferred": inferred,
                    "industry_inferred_confidence": confidence,
                    "industry_inferred_method": method,
                    "industry_inferred_at": now_iso,
                }
            )
        if writeback_industry and confidence >= min_confidence:
            payload["industry"] = inferred
            accepted += 1
        updates.append(payload)

    if not updates:
        return {
            "success": True,
            "message": "No inferable rows from current model.",
            "missing_rows": len(missing_rows),
            "updated": 0,
        }

    batch_size = 200
    updated = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        supabase.table("tickers").upsert(batch, on_conflict="ticker").execute()
        updated += len(batch)

    return {
        "success": True,
        "total_rows": len(all_rows),
        "labeled_rows": len(labeled_rows),
        "missing_rows": len(missing_rows),
        "updated_rows": updated,
        "accepted_writeback_rows": accepted,
        "min_confidence": min_confidence,
        "writeback_industry": writeback_industry,
        "supports_inferred_columns": supports_inferred_columns,
    }


if __name__ == "__main__":
    print(infer_and_backfill())
