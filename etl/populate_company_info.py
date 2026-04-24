import os
import re
import time
from vnstock import Listing, Company
from .config import get_supabase_client


def _clean_nullable_text(value):
    """Trả về None nếu value rỗng/nan để tránh lưu chuỗi 'nan'."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "<na>", "n/a", "unknown"}:
        return None
    return text


def _fetch_all_existing_tickers(supabase, page_size: int = 1000) -> list[dict]:
    """Lấy toàn bộ metadata ticker hiện có để tránh overwrite bằng dữ liệu thiếu."""
    all_rows = []
    offset = 0
    while True:
        res = (
            supabase.table("tickers")
            .select("ticker,industry,exchange,company_name,contact_phone")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = res.data or []
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_rows


def _load_company_profile_df():
    """
    Tương thích vnstock version mới:
    dùng Listing().all_symbols() để lấy profile (symbol, organ_name, icbName3, comGroupCode...).
    """
    listing = Listing()
    return listing.all_symbols()


def _build_symbol_value_map(data, value_hints: tuple[str, ...]) -> dict[str, str]:
    """
    Convert output linh hoạt (DataFrame/dict/list) -> map {symbol: value}.
    Hỗ trợ nhiều shape output của vnstock methods.
    """
    symbol_map: dict[str, str] = {}
    if data is None:
        return symbol_map

    # Case 1: pandas DataFrame-like
    if hasattr(data, "columns") and hasattr(data, "iterrows"):
        cols = [str(c) for c in list(data.columns)]
        symbol_col = None
        for c in ("symbol", "ticker", "code"):
            if c in cols:
                symbol_col = c
                break
        value_col = None
        for hint in value_hints:
            if hint in cols:
                value_col = hint
                break
        if not value_col and len(cols) >= 2:
            value_col = cols[1]

        if symbol_col and value_col:
            for _, row in data.iterrows():
                symbol = _clean_nullable_text(row.get(symbol_col))
                value = _clean_nullable_text(row.get(value_col))
                if symbol and value:
                    symbol_map[symbol] = value
        return symbol_map

    # Case 2: dict group->list[symbol] or dict symbol->value
    if isinstance(data, dict):
        for k, v in data.items():
            k_clean = _clean_nullable_text(k)
            if isinstance(v, (list, tuple, set)):
                for symbol in v:
                    s_clean = _clean_nullable_text(symbol)
                    if s_clean and k_clean:
                        symbol_map[s_clean] = k_clean
            else:
                v_clean = _clean_nullable_text(v)
                if k_clean and v_clean:
                    symbol_map[k_clean] = v_clean
        return symbol_map

    # Case 3: list of dict-like
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            symbol = _clean_nullable_text(item.get("symbol") or item.get("ticker") or item.get("code"))
            if not symbol:
                continue
            value = None
            for hint in value_hints:
                value = _clean_nullable_text(item.get(hint))
                if value:
                    break
            if not value:
                other_values = [vv for kk, vv in item.items() if kk not in ("symbol", "ticker", "code")]
                if other_values:
                    value = _clean_nullable_text(other_values[0])
            if value:
                symbol_map[symbol] = value
    return symbol_map


def _load_enrichment_maps() -> tuple[dict[str, str], dict[str, str]]:
    """Lấy map exchange/industry theo symbol từ các endpoint chuyên biệt."""
    listing = Listing()
    exchange_map: dict[str, str] = {}
    industry_map: dict[str, str] = {}

    try:
        raw_exchange = listing.symbols_by_exchange()
        exchange_map = _build_symbol_value_map(
            raw_exchange,
            value_hints=("exchange", "comGroupCode", "market", "exchange_name"),
        )
    except Exception:
        exchange_map = {}

    try:
        raw_industry = listing.symbols_by_industries()
        industry_map = _build_symbol_value_map(
            raw_industry,
            value_hints=("industry", "icbName3", "industry_name", "group"),
        )
    except Exception:
        industry_map = {}

    return exchange_map, industry_map


def _extract_first_non_empty(record: dict, keys: tuple[str, ...]):
    for k in keys:
        v = _clean_nullable_text(record.get(k))
        if v:
            return v
    return None


def _normalize_phone(value: str | None) -> str | None:
    text = _clean_nullable_text(value)
    if not text:
        return None
    # Keep only characters commonly used in phone formatting.
    normalized = re.sub(r"[^0-9+\-\s().]", "", text).strip()
    # Require at least 8 digits to avoid noisy captures.
    digit_count = len(re.sub(r"\D", "", normalized))
    if digit_count < 8:
        return None
    return normalized


def _fetch_overview_enrichment(ticker: str) -> tuple[str, str | None, str | None, str | None, str]:
    """Fallback enrichment theo từng mã qua Company.overview()."""
    try:
        df = Company(symbol=ticker).overview()
        if df is None or getattr(df, "empty", True):
            return ticker, None, None, None, ""
        row = df.iloc[0].to_dict()
        industry = _extract_first_non_empty(
            row,
            (
                "industry",
                "industry_name",
                "icb_name",
                "icb_name3",
                "icbName3",
                "level_3",
            ),
        )
        exchange = _extract_first_non_empty(
            row,
            (
                "exchange",
                "comGroupCode",
                "market",
                "exchange_name",
            ),
        )
        phone = _normalize_phone(
            _extract_first_non_empty(
                row,
                (
                    "phone",
                    "telephone",
                    "tel",
                    "hotline",
                    "company_phone",
                    "companyPhone",
                    "contact_phone",
                    "contactPhone",
                ),
            )
        )
        return ticker, industry, exchange, phone, ""
    except Exception as exc:
        return ticker, None, None, None, str(exc)


def _is_missing_industry(value) -> bool:
    text = _clean_nullable_text(value)
    if not text:
        return True
    return text.strip().upper() in {"N/A", "UNKNOWN"}


def _extract_wait_seconds(error_text: str, default_wait: int = 30) -> int:
    """Parse số giây cần chờ từ thông báo rate limit của vnstock."""
    if not error_text:
        return default_wait
    match = re.search(r"ch[oơ]\s+(\d+)\s+gi[âa]y", error_text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"wait\s+(\d+)\s+seconds?", error_text, flags=re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return default_wait
    return default_wait


def _fetch_overview_with_retry(ticker: str, max_attempts: int = 2) -> tuple[str, str | None, str | None, str | None]:
    """Gọi Company.overview() có xử lý rate limit mềm."""
    for attempt in range(1, max_attempts + 1):
        t, industry, exchange, phone, error_text = _fetch_overview_enrichment(ticker)
        if industry or exchange or phone:
            return t, industry, exchange, phone
        if attempt < max_attempts:
            wait_s = _extract_wait_seconds(error_text, default_wait=30)
            time.sleep(wait_s)
    return ticker, None, None, None


def enrich_tickers_with_company_info():
    """
    Data Enrichment:
    Lấy hồ sơ doanh nghiệp và cập nhật trực tiếp vào bảng tickers
    để đảm bảo cột industry/company_name đầy đủ cho BI.
    """
    print("[ENRICH] Lay ho so doanh nghiep (industry/company_name/exchange)...")
    supabase = get_supabase_client()
    existing_rows = _fetch_all_existing_tickers(supabase)
    existing_map = {row["ticker"]: row for row in existing_rows if row.get("ticker")}

    df = _load_company_profile_df()
    exchange_map, industry_map = _load_enrichment_maps()
    if df is None or df.empty:
        return {
            "step": "enrich_company_info",
            "success": False,
            "records_fetched": 0,
            "records_upserted": 0,
            "errors": 1,
            "error_rate": 1.0,
            "message": "Listing().all_symbols() trả về rỗng.",
        }

    if "ticker" not in df.columns and "symbol" in df.columns:
        df["ticker"] = df["symbol"]
    if "ticker" not in df.columns:
        return {
            "step": "enrich_company_info",
            "success": False,
            "records_fetched": 0,
            "records_upserted": 0,
            "errors": 1,
            "error_rate": 1.0,
            "message": f"Thiếu cột ticker. Columns={list(df.columns)}",
        }

    if "icbName3" not in df.columns and "industry" in df.columns:
        df["icbName3"] = df["industry"]
    if "icbName3" not in df.columns:
        df["icbName3"] = None
    if "organName" not in df.columns and "organ_name" in df.columns:
        df["organName"] = df["organ_name"]
    if "organName" not in df.columns:
        df["organName"] = df.get("organShortName", df["ticker"])
    if "comGroupCode" not in df.columns and "exchange" in df.columns:
        df["comGroupCode"] = df["exchange"]
    if "comGroupCode" not in df.columns:
        df["comGroupCode"] = None

    records_to_upsert = []
    missing_industry_tickers = []
    missing_phone_tickers = []
    for _, row in df.iterrows():
        ticker = row.get("ticker")
        if not ticker:
            continue
        clean_ticker = _clean_nullable_text(ticker)
        if not clean_ticker:
            continue
        existing = existing_map.get(clean_ticker, {})
        industry_value = (
            industry_map.get(clean_ticker)
            or _clean_nullable_text(row.get("icbName3"))
            or _clean_nullable_text(existing.get("industry"))
        )
        exchange_value = (
            exchange_map.get(clean_ticker)
            or _clean_nullable_text(row.get("comGroupCode"))
            or _clean_nullable_text(existing.get("exchange"))
            or "UNKNOWN"
        )
        company_name_value = _clean_nullable_text(row.get("organName")) or _clean_nullable_text(existing.get("company_name"))
        contact_phone_value = (
            _normalize_phone(
                _extract_first_non_empty(
                    row.to_dict() if hasattr(row, "to_dict") else {},
                    (
                        "phone",
                        "telephone",
                        "tel",
                        "hotline",
                        "company_phone",
                        "companyPhone",
                        "contact_phone",
                        "contactPhone",
                    ),
                )
            )
            or _normalize_phone(existing.get("contact_phone"))
        )
        if _is_missing_industry(industry_value):
            missing_industry_tickers.append(clean_ticker)
        if not contact_phone_value:
            missing_phone_tickers.append(clean_ticker)

        records_to_upsert.append(
            {
                "ticker": clean_ticker,
                "company_name": company_name_value,
                "industry": industry_value,
                "exchange": exchange_value,
                "contact_phone": contact_phone_value,
            }
        )

    # Fallback layer: enrich industry/exchange/phone bằng Company.overview().
    # Cho phép chạy full tự động theo nhu cầu (có throttle để tránh rate limit).
    fallback_candidates = sorted(set(missing_industry_tickers + missing_phone_tickers))
    if fallback_candidates:
        max_fallback_tickers = int(os.getenv("ENRICH_OVERVIEW_MAX_TICKERS", "3000"))
        max_requests_per_minute = int(os.getenv("ENRICH_OVERVIEW_MAX_RPM", "18"))
        delay_s = max(60.0 / max_requests_per_minute, 0.0)
        fallback_targets = fallback_candidates[:max_fallback_tickers]

        print(
            f"[ENRICH] Fallback Company.overview() cho {len(fallback_targets)}/{len(fallback_candidates)} "
            "ma thieu nganh/so dien thoai..."
        )
        if len(fallback_candidates) > max_fallback_tickers:
            print(
                f"[ENRICH] Con {len(fallback_candidates) - max_fallback_tickers} ma chua enrich o luot nay "
                f"(gioi han ENRICH_OVERVIEW_MAX_TICKERS={max_fallback_tickers})."
            )

        fallback_map = {}
        for idx, ticker in enumerate(fallback_targets, start=1):
            t, industry, exchange, phone = _fetch_overview_with_retry(ticker, max_attempts=2)
            fallback_map[t] = {"industry": industry, "exchange": exchange, "contact_phone": phone}
            if idx < len(fallback_targets) and delay_s > 0:
                time.sleep(delay_s)

        for rec in records_to_upsert:
            ticker = rec["ticker"]
            fb = fallback_map.get(ticker)
            if not fb:
                continue
            if _is_missing_industry(rec.get("industry")) and fb.get("industry"):
                rec["industry"] = fb["industry"]
            if _clean_nullable_text(rec.get("exchange")) in {None, "UNKNOWN"} and fb.get("exchange"):
                rec["exchange"] = fb["exchange"]
            if not _normalize_phone(rec.get("contact_phone")) and fb.get("contact_phone"):
                rec["contact_phone"] = _normalize_phone(fb["contact_phone"])

    upserted_count = 0
    batch_size = 100
    for i in range(0, len(records_to_upsert), batch_size):
        batch = records_to_upsert[i:i + batch_size]
        supabase.table("tickers").upsert(batch, on_conflict="ticker").execute()
        upserted_count += len(batch)

    print(f"[ENRICH] Hoan tat: {upserted_count} ma duoc cap nhat vao bang tickers.")
    missing_after = sum(1 for r in records_to_upsert if _is_missing_industry(r.get("industry")))
    missing_phone_after = sum(1 for r in records_to_upsert if not _normalize_phone(r.get("contact_phone")))
    return {
        "step": "enrich_company_info",
        "success": True,
        "records_fetched": len(records_to_upsert),
        "records_upserted": upserted_count,
        "industry_missing_after": missing_after,
        "contact_phone_missing_after": missing_phone_after,
        "errors": 0,
        "error_rate": 0.0,
    }

if __name__ == "__main__":
    enrich_tickers_with_company_info()
