# File: etl/ai_agent.py
import os
import json
import re
import requests
from dotenv import load_dotenv
from .config import get_supabase_client

load_dotenv()

PROMPT_SQL_PLANNER = """
[ROLE]
Ban la Senior Financial Data Analyst + PostgreSQL Translator cho thi truong chung khoan Viet Nam.

[MISSION]
Tu cau hoi nguoi dung, tao DUY NHAT 1 cau SQL PostgreSQL de truy van du lieu chinh xac.

[HARD RULES]
- Chi tra ve SQL thuan, khong markdown, khong giai thich.
- Chi cho phep SELECT hoac WITH ... SELECT.
- Chi dung bang: daily_prices, financial_reports, tickers.
- Khong dung INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE...
- Neu cau hoi theo ngay => dung trading_date.
- Neu hoi "lai nhat":
  1) profit_after_tax (tuyet doi), 2) roe, 3) net_margin.
- Neu khong chi ro ky bao cao tai chinh:
  report_type='yearly' va period moi nhat.
- Cau hoi co "lon nhat/nho nhat/top" phai co ORDER BY + LIMIT.
- Neu cau hoi co "volume/khoi luong", SELECT bat buoc co cot volume.
- Neu cau hoi co "gia", uu tien close_price (hoac open/high/low dung ngu canh).
- Tranh SELECT *.

[SCHEMA]
{schema_definition}

[USER QUESTION]
{user_question}
"""

PROMPT_SQL_CRITIC = """
Ban la SQL Reviewer.

Dau vao:
- Cau hoi: {user_question}
- SQL de xuat: {candidate_sql}
- Cac cot bat buoc neu co: {required_columns}

Nhiem vu:
1) Kiem tra SQL co tra loi truc tiep cau hoi khong.
2) Kiem tra SQL co thieu metric/cot bat buoc khong.
3) Kiem tra SQL co an toan read-only khong.
4) Neu chua dat, sua lai SQL.
5) Chi tra ve SQL cuoi cung, khong giai thich.
"""

PROMPT_ANALYST_NARRATOR = """
[ROLE]
Ban la chuyen gia phan tich tai chinh trung lap tai Viet Nam.

[INPUT]
- Cau hoi user: {user_question}
- SQL da chay: {safe_sql}
- Du lieu tra ve (JSON): {rows_json}
- Canh bao: {sensitive_note}

[OUTPUT REQUIREMENTS]
- Chi dung du lieu da cho, khong suy dien ngoai du lieu.
- Neu du lieu thieu, noi ro gioi han du lieu.
- Dung thuat ngu nghiep vu.
- Khong khuyen nghi mua/ban truc tiep.
- Neu co du lieu, trich 2-4 con so then chot.
- Toi da 180 tu.

[FORMAT]
[Tom tat du kien]
- ...
[Phan tich trung lap]
- ...
[Rui ro/Gioi han du lieu]
- ...
"""


def _extract_ai_text(result_json: dict) -> str | None:
    """Lấy text an toàn từ Gemini response."""
    candidates = result_json.get("candidates", [])
    if not candidates:
        return None
    content = candidates[0].get("content", {})
    parts = content.get("parts", [])
    if not parts:
        return None
    text = parts[0].get("text", "")
    return text.strip() if text else None


def _discover_generate_models(api_key: str) -> list[str]:
    """
    Gọi ListModels để lấy danh sách model thực sự hỗ trợ generateContent.
    Trả về tên dạng 'gemini-xxx' (không có prefix models/).
    """
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
        discovered = []
        for m in data.get("models", []):
            methods = m.get("supportedGenerationMethods", []) or []
            if "generateContent" not in methods:
                continue
            name = m.get("name", "")
            if name.startswith("models/"):
                name = name.split("/", 1)[1]
            if name:
                discovered.append(name)
        return discovered
    except Exception:
        return []


def _build_candidate_models(api_key: str) -> list[str]:
    """Ưu tiên model từ env, sau đó fallback theo list động từ API."""
    models_from_env = os.getenv("GEMINI_MODELS", "").strip()
    env_models = [m.strip() for m in models_from_env.split(",") if m.strip()] if models_from_env else []
    discovered_models = _discover_generate_models(api_key)

    # Ưu tiên flash trước để tiết kiệm chi phí/thời gian
    flash_models = [m for m in discovered_models if "flash" in m]
    non_flash_models = [m for m in discovered_models if "flash" not in m]
    merged = env_models + flash_models + non_flash_models

    # Dedup giữ thứ tự
    seen = set()
    ordered = []
    for model in merged:
        if model in seen:
            continue
        seen.add(model)
        ordered.append(model)
    return ordered


def _call_gemini_text(prompt: str, temperature: float = 0.35) -> tuple[str | None, list[str]]:
    """Call Gemini and return plain text response + error traces."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return None, ["GEMINI_API_KEY is missing"]
    max_attempts = int(os.getenv("GEMINI_CALL_MAX_ATTEMPTS", "3"))
    request_timeout_s = int(os.getenv("GEMINI_CALL_TIMEOUT_SECONDS", "30"))
    retry_base_sleep_s = float(os.getenv("GEMINI_RETRY_BASE_SLEEP_SECONDS", "1.5"))

    candidate_models = _build_candidate_models(api_key)
    if not candidate_models:
        return None, ["No generateContent model discovered from ListModels"]

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature},
    }
    headers = {"Content-Type": "application/json"}

    errors = []
    for model_name in candidate_models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=request_timeout_s)
                if response.status_code == 200:
                    result = response.json()
                    ai_text = _extract_ai_text(result)
                    if ai_text:
                        return ai_text, errors
                    errors.append(f"{model_name}: response has no valid text")
                    break
                if response.status_code == 404:
                    errors.append(f"{model_name}: 404 model not found")
                    break
                # Retry transient limits/availability
                if response.status_code in (408, 429, 500, 502, 503, 504) and attempt < max_attempts:
                    sleep_s = retry_base_sleep_s * (2 ** (attempt - 1))
                    errors.append(
                        f"{model_name}: transient HTTP {response.status_code}, retry {attempt}/{max_attempts} in {sleep_s:.1f}s"
                    )
                    import time
                    time.sleep(sleep_s)
                    continue
                errors.append(f"{model_name}: HTTP {response.status_code} - {response.text[:200]}")
                break
            except requests.exceptions.Timeout:
                if attempt < max_attempts:
                    sleep_s = retry_base_sleep_s * (2 ** (attempt - 1))
                    errors.append(
                        f"{model_name}: timeout, retry {attempt}/{max_attempts} in {sleep_s:.1f}s"
                    )
                    import time
                    time.sleep(sleep_s)
                    continue
                errors.append(f"{model_name}: timeout after {max_attempts} attempts")
                break
            except requests.exceptions.ConnectionError as exc:
                if attempt < max_attempts:
                    sleep_s = retry_base_sleep_s * (2 ** (attempt - 1))
                    errors.append(
                        f"{model_name}: connection error ({exc}), retry {attempt}/{max_attempts} in {sleep_s:.1f}s"
                    )
                    import time
                    time.sleep(sleep_s)
                    continue
                errors.append(f"{model_name}: connection error - {exc}")
                break
            except Exception as exc:
                errors.append(f"{model_name}: unexpected error - {exc}")
                break
    return None, errors


def _load_data_dictionary_text() -> str:
    """Load DATA_DICTIONARY.md to ground SQL generation."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dict_path = os.path.join(project_root, "DATA_DICTIONARY.md")
    try:
        with open(data_dict_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return (
            "Tables: tickers(ticker,exchange,industry,company_name), "
            "daily_prices(ticker,trading_date,open_price,high_price,low_price,close_price,volume), "
            "financial_reports(ticker,report_type,period,revenue,profit_after_tax,roe,roa,debt_to_equity,"
            "net_margin,gross_margin,operating_margin,cash_flow_operating,interest_expense)."
        )


def _extract_sql_from_model_text(model_text: str) -> str:
    """Extract SQL from markdown fences or plain text."""
    if not model_text:
        return ""
    text = model_text.strip()
    match = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
    if match:
        text = match.group(1).strip()
    return text.strip()


def _validate_readonly_sql(sql: str) -> tuple[bool, str]:
    """Strict validation for read-only, single-statement SQL."""
    if not sql:
        return False, "SQL is empty"
    normalized = sql.strip()
    normalized = re.sub(r";+\s*$", "", normalized)
    if ";" in normalized:
        return False, "Only one SQL statement is allowed"
    if not re.match(r"^(select|with)\s", normalized, flags=re.IGNORECASE):
        return False, "Only SELECT/CTE queries are allowed"

    forbidden = (
        "insert", "update", "delete", "drop", "alter", "create", "truncate",
        "grant", "revoke", "comment", "copy", "vacuum", "analyze", "refresh", "call", "do",
    )
    if re.search(r"\b(" + "|".join(forbidden) + r")\b", normalized, flags=re.IGNORECASE):
        return False, "Detected forbidden keyword for non-readonly operation"

    if re.search(r"\b(pg_|information_schema)\w*", normalized, flags=re.IGNORECASE):
        return False, "System catalog access is not allowed"

    if not re.search(r"\b(daily_prices|financial_reports|tickers)\b", normalized, flags=re.IGNORECASE):
        return False, "Query must target allowed business tables"

    return True, normalized


def _execute_readonly_sql(sql: str) -> list[dict]:
    """
    Execute read-only SQL through Supabase RPC.
    Requires DB function: public.execute_readonly_sql(p_sql text).
    """
    client = get_supabase_client()
    try:
        resp = client.rpc("execute_readonly_sql", {"p_sql": sql}).execute()
    except Exception as exc:
        raise RuntimeError(
            "RPC execute_readonly_sql not available or failed. "
            "Apply migration scripts/04_create_execute_readonly_sql_rpc.sql first."
        ) from exc

    data = resp.data or []
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return []

    rows = []
    for item in data:
        # Many RPC implementations return jsonb as {"to_jsonb": {...}} or directly dict.
        if isinstance(item, dict) and "to_jsonb" in item and isinstance(item["to_jsonb"], dict):
            rows.append(item["to_jsonb"])
        elif isinstance(item, dict):
            rows.append(item)
    return rows


def _is_sensitive_investment_question(user_question: str) -> bool:
    text = (user_question or "").lower()
    return any(k in text for k in ["nen mua", "nen ban", "co nen", "dau tu", "all-in", "chot loi"])


def _required_columns_from_question(user_question: str) -> list[str]:
    text = (user_question or "").lower()
    required = []
    if any(k in text for k in ["volume", "khoi luong", "thanh khoan"]):
        required.append("volume")
    if "gia" in text:
        required.append("close_price")
    if "roe" in text:
        required.append("roe")
    if "roa" in text:
        required.append("roa")
    if "net margin" in text or "bien loi nhuan rong" in text:
        required.append("net_margin")
    if any(
        k in text
        for k in [
            "gross margin",
            "bien loi nhuan gop",
            "biên lợi nhuận gộp",
            "loi nhuan gop",
            "lợi nhuận gộp",
        ]
    ):
        required.append("gross_margin")
    if "debt" in text or "don bay" in text or "no" in text:
        required.append("debt_to_equity")
    if any(k in text for k in ["rui ro", "rủi ro", "an toan", "an toàn", "it rui ro", "ít rủi ro"]):
        # For "low-risk" screening, always require a leverage proxy.
        if "debt_to_equity" not in required:
            required.append("debt_to_equity")
    return required


def _sql_has_required_columns(sql: str, required_columns: list[str]) -> tuple[bool, str]:
    lowered = (sql or "").lower()
    for col in required_columns:
        if re.search(rf"\b{re.escape(col.lower())}\b", lowered) is None:
            return False, f"SQL missing required column: {col}"
    return True, ""


def _question_mentions_banking(user_question: str) -> bool:
    text = (user_question or "").lower()
    return any(k in text for k in ["ngân hàng", "ngan hang", "bank", "banking"])


def _sql_has_banking_filter(sql: str) -> bool:
    lowered = (sql or "").lower()
    # Detect common filtering patterns for industry bank.
    return (
        ("industry" in lowered and ("ngan hang" in lowered or "ngân hàng" in lowered or "bank" in lowered))
        or ("coalesce(industry" in lowered and ("ngan hang" in lowered or "bank" in lowered))
    )


def _is_ranking_question(user_question: str) -> bool:
    text = (user_question or "").lower()
    return any(
        k in text
        for k in ["tot nhat", "tốt nhất", "cao nhat", "cao nhất", "lon nhat", "lớn nhất", "best", "top"]
    )


def _pick_ranking_metric(required_columns: list[str]) -> str | None:
    for metric in ["gross_margin", "roe", "roa", "net_margin", "profit_after_tax", "volume", "close_price"]:
        if metric in required_columns:
            return metric
    return None


def _is_null_like(value) -> bool:
    return value is None or str(value).strip().lower() in {"none", "null", "nan", ""}


def _question_has_explicit_date(user_question: str) -> bool:
    text = (user_question or "").lower()
    if re.search(r"\b20\d{2}-\d{2}-\d{2}\b", text):
        return True
    if re.search(r"\b\d{1,2}/\d{1,2}/20\d{2}\b", text):
        return True
    if any(k in text for k in ["hôm nay", "hom nay", "hôm qua", "hom qua", "latest", "gần nhất", "gan nhat"]):
        return True
    return False


def _sql_targets_daily_prices(sql: str) -> bool:
    return re.search(r"\bdaily_prices\b", (sql or "").lower()) is not None


def _sql_has_trading_date_filter(sql: str) -> bool:
    lowered = (sql or "").lower()
    # Accept common date filter styles.
    return re.search(
        r"\btrading_date\b\s*(=|>|<|>=|<=|between|in)\b|\bwhere\b[\s\S]*\btrading_date\b",
        lowered,
        flags=re.IGNORECASE,
    ) is not None


def chatbot_text_to_sql_flow(user_question: str) -> dict:
    """
    Text-to-SQL Agent (3 phases):
    1) NL question -> SQL
    2) SQL execution on Supabase
    3) Raw rows -> analyst-style neutral answer
    """
    if not user_question or not user_question.strip():
        return {
            "success": False,
            "error": "Question is empty",
            "sql": None,
            "rows": [],
            "answer": "Vui long nhap cau hoi.",
        }

    schema_definition = _load_data_dictionary_text()
    prompt_sql = PROMPT_SQL_PLANNER.format(
        schema_definition=schema_definition,
        user_question=user_question,
    )
    required_columns = _required_columns_from_question(user_question)
    raw_sql_text, sql_errors = _call_gemini_text(prompt_sql, temperature=0.0)
    if not raw_sql_text:
        return {
            "success": False,
            "error": "Failed to generate SQL",
            "sql": None,
            "rows": [],
            "answer": "[AI ERROR] Khong sinh duoc SQL. " + " | ".join(sql_errors[:3]),
        }

    extracted_sql = _extract_sql_from_model_text(raw_sql_text)
    is_valid, sql_or_error = _validate_readonly_sql(extracted_sql)
    if not is_valid:
        return {
            "success": False,
            "error": sql_or_error,
            "sql": extracted_sql,
            "rows": [],
            "answer": f"[AI ERROR] SQL khong hop le: {sql_or_error}",
        }
    safe_sql = sql_or_error

    # Fitness check: if missing required business columns, ask critic to regenerate once.
    fit_ok, fit_error = _sql_has_required_columns(safe_sql, required_columns)
    if not fit_ok:
        prompt_critic = PROMPT_SQL_CRITIC.format(
            user_question=user_question,
            candidate_sql=safe_sql,
            required_columns=", ".join(required_columns) if required_columns else "(none)",
        )
        critic_text, critic_errors = _call_gemini_text(prompt_critic, temperature=0.0)
        if not critic_text:
            return {
                "success": False,
                "error": fit_error,
                "sql": safe_sql,
                "rows": [],
                "answer": "[AI ERROR] SQL chua dat business intent va khong regenerate duoc. "
                + " | ".join(critic_errors[:3]),
            }
        revised_sql = _extract_sql_from_model_text(critic_text)
        revised_valid, revised_or_error = _validate_readonly_sql(revised_sql)
        if not revised_valid:
            return {
                "success": False,
                "error": revised_or_error,
                "sql": revised_sql,
                "rows": [],
                "answer": f"[AI ERROR] SQL sau critic khong hop le: {revised_or_error}",
            }
        revised_fit_ok, revised_fit_error = _sql_has_required_columns(revised_or_error, required_columns)
        if not revised_fit_ok:
            return {
                "success": False,
                "error": revised_fit_error,
                "sql": revised_or_error,
                "rows": [],
                "answer": f"[AI ERROR] SQL sau critic van chua dat: {revised_fit_error}",
            }
        safe_sql = revised_or_error

    # Domain guardrail: if question asks for banking sector, force robust bank-industry filter.
    if _question_mentions_banking(user_question) and not _sql_has_banking_filter(safe_sql):
        prompt_banking_filter = (
            "Cau hoi yeu cau pham vi NGAN HANG, nhung SQL hien tai chua co bo loc nganh ngan hang du ro rang. "
            "Hay sua SQL de chi lay cac ticker thuoc nganh ngan hang bang cach join bang tickers "
            "va loc industry theo bien the text robust (ngan hang/bank). "
            "Giu nguyen business intent xep hang va LIMIT.\n"
            f"Cau hoi: {user_question}\n"
            f"SQL hien tai: {safe_sql}\n"
            "Chi tra ve SQL cuoi cung."
        )
        banking_text, banking_errors = _call_gemini_text(prompt_banking_filter, temperature=0.0)
        if not banking_text:
            return {
                "success": False,
                "error": "Cannot enforce banking industry scope",
                "sql": safe_sql,
                "rows": [],
                "answer": "[AI ERROR] Khong the bo sung bo loc nganh ngan hang. "
                + " | ".join(banking_errors[:3]),
            }
        banking_sql = _extract_sql_from_model_text(banking_text)
        banking_valid, banking_or_error = _validate_readonly_sql(banking_sql)
        if not banking_valid:
            return {
                "success": False,
                "error": banking_or_error,
                "sql": banking_sql,
                "rows": [],
                "answer": f"[AI ERROR] SQL bo sung bo loc ngan hang khong hop le: {banking_or_error}",
            }
        # Ensure required columns are still present.
        banking_fit_ok, banking_fit_error = _sql_has_required_columns(banking_or_error, required_columns)
        if not banking_fit_ok:
            return {
                "success": False,
                "error": banking_fit_error,
                "sql": banking_or_error,
                "rows": [],
                "answer": f"[AI ERROR] SQL ngan hang thieu cot bat buoc: {banking_fit_error}",
            }
        safe_sql = banking_or_error

    # Default guardrail: if question does not specify date but query hits daily_prices,
    # enforce trading_date = latest_date to avoid accidental all-time query.
    if _sql_targets_daily_prices(safe_sql) and not _question_has_explicit_date(user_question) and not _sql_has_trading_date_filter(safe_sql):
        prompt_latest_date = (
            "Ban can sua SQL de them filter trading_date = (SELECT MAX(trading_date) FROM daily_prices) "
            "neu query dung daily_prices ma cau hoi khong noi ro ngay. "
            "Giu nguyen business intent va cac cot bat buoc.\n"
            f"Cau hoi: {user_question}\n"
            f"SQL hien tai: {safe_sql}\n"
            f"Cot bat buoc: {', '.join(required_columns) if required_columns else '(none)'}\n"
            "Chi tra ve SQL cuoi cung."
        )
        latest_text, latest_errors = _call_gemini_text(prompt_latest_date, temperature=0.0)
        if not latest_text:
            return {
                "success": False,
                "error": "Cannot enforce latest trading_date",
                "sql": safe_sql,
                "rows": [],
                "answer": "[AI ERROR] Khong the bo sung filter latest trading_date. "
                + " | ".join(latest_errors[:3]),
            }
        latest_sql = _extract_sql_from_model_text(latest_text)
        latest_valid, latest_or_error = _validate_readonly_sql(latest_sql)
        if not latest_valid:
            return {
                "success": False,
                "error": latest_or_error,
                "sql": latest_sql,
                "rows": [],
                "answer": f"[AI ERROR] SQL bo sung latest_date khong hop le: {latest_or_error}",
            }
        latest_fit_ok, latest_fit_error = _sql_has_required_columns(latest_or_error, required_columns)
        if not latest_fit_ok:
            return {
                "success": False,
                "error": latest_fit_error,
                "sql": latest_or_error,
                "rows": [],
                "answer": f"[AI ERROR] SQL latest_date thieu cot bat buoc: {latest_fit_error}",
            }
        if not _sql_has_trading_date_filter(latest_or_error):
            return {
                "success": False,
                "error": "latest_date filter is still missing",
                "sql": latest_or_error,
                "rows": [],
                "answer": "[AI ERROR] SQL sau enforce van thieu dieu kien trading_date.",
            }
        safe_sql = latest_or_error

    try:
        rows = _execute_readonly_sql(safe_sql)
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "sql": safe_sql,
            "rows": [],
            "answer": f"[DB ERROR] {exc}",
        }

    # Ranking guardrail for financial metrics:
    # If user asks "best/top" and top row has null metric, regenerate SQL with IS NOT NULL + NULLS LAST.
    ranking_metric = _pick_ranking_metric(required_columns)
    if _is_ranking_question(user_question) and ranking_metric and rows:
        first_row = rows[0] if isinstance(rows[0], dict) else {}
        if _is_null_like(first_row.get(ranking_metric)):
            prompt_non_null_rank = (
                "SQL hien tai tra ve dong dau co metric bi null, khong dung business intent 'top/best'. "
                f"Hay sua SQL de loai bo null metric {ranking_metric} bang dieu kien `{ranking_metric} IS NOT NULL`, "
                f"va khi sap xep thi dung `ORDER BY {ranking_metric} DESC NULLS LAST`. "
                "Giu nguyen intent cau hoi, bang du lieu, bo loc thoi gian, va LIMIT.\n"
                f"Cau hoi: {user_question}\n"
                f"SQL hien tai: {safe_sql}\n"
                "Chi tra ve SQL cuoi cung."
            )
            revised_text, revised_errors = _call_gemini_text(prompt_non_null_rank, temperature=0.0)
            if revised_text:
                revised_sql = _extract_sql_from_model_text(revised_text)
                revised_valid, revised_or_error = _validate_readonly_sql(revised_sql)
                if revised_valid:
                    try:
                        revised_rows = _execute_readonly_sql(revised_or_error)
                        if revised_rows:
                            safe_sql = revised_or_error
                            rows = revised_rows
                    except Exception:
                        pass
            else:
                # Keep current result but allow narrator to expose data limitation.
                _ = revised_errors

    rows_preview = rows[:50]
    sensitive_note = (
        "User question co tinh chat khuyen nghi dau tu. "
        "Bat buoc tra loi theo huong screening trung lap, neu ro rui ro, KHONG dua lenh mua/ban."
        if _is_sensitive_investment_question(user_question)
        else "Tra loi trung lap va data-driven."
    )
    prompt_answer = PROMPT_ANALYST_NARRATOR.format(
        user_question=user_question,
        safe_sql=safe_sql,
        rows_json=json.dumps(rows_preview, ensure_ascii=False),
        sensitive_note=sensitive_note,
    )
    final_answer, answer_errors = _call_gemini_text(prompt_answer, temperature=0.25)
    if not final_answer:
        final_answer = "[AI ERROR] Khong tong hop duoc cau tra loi. " + " | ".join(answer_errors[:3])

    return {
        "success": True,
        "error": None,
        "sql": safe_sql,
        "rows": rows_preview,
        "answer": final_answer,
    }


def _build_market_data_context(df_top) -> str:
    """Chuẩn hóa input top thanh khoản thành text gọn để AI bám dữ liệu."""
    if df_top is None:
        return "Khong co du lieu top thanh khoan."
    try:
        cols = list(df_top.columns)
    except Exception:
        return "Khong doc duoc cau truc du lieu dau vao."

    preferred_cols = [
        "ticker",
        "trading_date",
        "close_price",
        "open_price",
        "high_price",
        "low_price",
        "volume",
        "industry",
        "net_margin",
        "gross_margin",
        "operating_margin",
        "roe",
        "roa",
        "debt_to_equity",
        "interest_expense",
        "cash_flow_operating",
        "inventory",
        "selling_expense",
    ]
    selected_cols = [c for c in preferred_cols if c in cols]
    if not selected_cols:
        selected_cols = cols[: min(len(cols), 8)]

    sample = df_top[selected_cols].copy()
    if len(sample) > 5:
        sample = sample.head(5)
    return sample.to_string(index=False)


def _compute_technical_snapshot(price_rows: list[dict]) -> dict:
    """Tạo snapshot kỹ thuật đơn giản từ cửa sổ giá gần nhất."""
    if not price_rows:
        return {}

    closes = [float(r.get("close_price", 0) or 0) for r in price_rows]
    highs = [float(r.get("high_price", 0) or 0) for r in price_rows]
    lows = [float(r.get("low_price", 0) or 0) for r in price_rows]
    volumes = [float(r.get("volume", 0) or 0) for r in price_rows]
    latest_close = closes[0] if closes else None
    support = min(lows) if lows else None
    resistance = max(highs) if highs else None
    avg_volume = (sum(volumes) / len(volumes)) if volumes else None

    return {
        "close_latest": latest_close,
        "support_20d": support,
        "resistance_20d": resistance,
        "avg_volume_20d": avg_volume,
    }


def _fetch_analyst_snapshots(df_top) -> list[dict]:
    """
    Enrich top tickers bằng:
    - Technicals từ daily_prices 20 phiên gần nhất
    - Fundamentals từ financial_reports (báo cáo năm mới nhất)
    """
    if df_top is None or getattr(df_top, "empty", True):
        return []

    tickers = []
    for _, row in df_top.iterrows():
        t = str(row.get("ticker", "")).strip()
        if t:
            tickers.append(t)
    tickers = list(dict.fromkeys(tickers))[:5]
    if not tickers:
        return []

    client = get_supabase_client()
    snapshots = []

    for ticker in tickers:
        # Technical snapshot
        px_resp = (
            client.table("daily_prices")
            .select("trading_date,close_price,high_price,low_price,volume")
            .eq("ticker", ticker)
            .order("trading_date", desc=True)
            .limit(20)
            .execute()
        )
        px_rows = px_resp.data or []
        technical = _compute_technical_snapshot(px_rows)

        # Fundamental snapshot (latest yearly)
        fin_resp = (
            client.table("financial_reports")
            .select(
                "period,revenue,gross_margin,operating_margin,net_margin,roe,roa,"
                "debt_to_equity,interest_expense,cash_flow_operating,inventory,selling_expense"
            )
            .eq("ticker", ticker)
            .eq("report_type", "yearly")
            .order("period", desc=True)
            .limit(1)
            .execute()
        )
        fin = (fin_resp.data or [{}])[0]

        snapshots.append(
            {
                "ticker": ticker,
                "latest_period": fin.get("period"),
                "revenue_bn_vnd": fin.get("revenue"),
                "gross_margin": fin.get("gross_margin"),
                "operating_margin": fin.get("operating_margin"),
                "net_margin": fin.get("net_margin"),
                "roe": fin.get("roe"),
                "roa": fin.get("roa"),
                "debt_to_equity": fin.get("debt_to_equity"),
                "interest_expense_bn_vnd": fin.get("interest_expense"),
                "cash_flow_operating_bn_vnd": fin.get("cash_flow_operating"),
                "inventory_bn_vnd": fin.get("inventory"),
                "selling_expense_bn_vnd": fin.get("selling_expense"),
                "close_latest": technical.get("close_latest"),
                "support_20d": technical.get("support_20d"),
                "resistance_20d": technical.get("resistance_20d"),
                "avg_volume_20d": technical.get("avg_volume_20d"),
            }
        )

    return snapshots


def _build_analyst_snapshot_context(df_top) -> str:
    """Convert analyst snapshots thành bảng text cho prompt."""
    try:
        snapshots = _fetch_analyst_snapshots(df_top)
        if not snapshots:
            return "Khong lay duoc analyst snapshot tu database."
        headers = [
            "ticker",
            "latest_period",
            "close_latest",
            "support_20d",
            "resistance_20d",
            "avg_volume_20d",
            "net_margin",
            "roe",
            "roa",
            "debt_to_equity",
            "cash_flow_operating_bn_vnd",
            "interest_expense_bn_vnd",
        ]
        lines = [" | ".join(headers)]
        for row in snapshots:
            lines.append(" | ".join(str(row.get(h, "")) for h in headers))
        return "\n".join(lines)
    except Exception as exc:
        return f"Khong lay duoc analyst snapshot: {exc}"


def get_ai_market_summary(df_top):
    """Dùng Gemini AI để phân tích và viết nhận định thị trường dựa trên dữ liệu giá."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return "[AI WARNING] Chua cau hinh GEMINI_API_KEY."

    print("[AI] Dang phan tich du lieu thi truong...")
    
    # 1. Chuyển đổi dữ liệu Top 5 thành văn bản để mớm cho AI
    data_str = _build_market_data_context(df_top)
    snapshot_str = _build_analyst_snapshot_context(df_top)
    
    # 2. Xây dựng System Prompt (Ép AI đóng vai chuyên gia)
    prompt = f"""
Ban la chuyen gia phan tich tai chinh trung lap cho thi truong chung khoan Viet Nam.

Du lieu dau vao (Top co phieu thanh khoan):
{data_str}

Du lieu bo sung cho phan tich analyst (fundamental + technical):
{snapshot_str}

Yeu cau bat buoc:
1) Viet theo nguyen tac "Noi co sach, mach co data":
- Dung so lieu tuyet doi/tuong doi khi co trong du lieu.
- Khong dung tinh tu cam xuc (vi du: "sieu tot", "qua manh", "bung no").

2) Dung thuat ngu nghiep vu:
- Uu tien cac term: net margin, gross margin, operating margin, ROE, ROA,
  debt-to-equity, interest expense, cash_flow_operating, consolidation, support/resistance.
- Neu khong co metric nao thi ghi ro: "chua du du lieu de ket luan metric nay".

3) Khung phan tich da chieu (khong dua khuyen nghi mua/ban):
- Tailwinds/Catalysts: yeu to ho tro.
- Headwinds/Risks: rui ro ngan/trung han.
- Technicals: cung-cau, vung ho tro/khang cu, trang thai tich luy hay phan phoi.

4) Dinh dang output dung mau sau (tieng Viet, toi da 130-170 tu):
[Tong quan]
- ...
[Tailwinds/Catalysts]
- ...
[Headwinds/Risks]
- ...
[Technicals]
- ...
[Ket luan trung lap]
- ...

5) Cam ket trung lap:
- Khong su dung tu "nen mua", "nen ban", "all-in", "chac chan".
- Neu du lieu han che, phai neu ro gia dinh va gioi han du lieu.
"""
    
    # 3. Call Gemini
    ai_text, errors = _call_gemini_text(prompt, temperature=0.35)
    if ai_text:
        return f"Goc nhin AI (Trung lap):\n{ai_text}"
    return "[AI ERROR] Khong goi duoc model phu hop. " + " | ".join(errors[:3])
