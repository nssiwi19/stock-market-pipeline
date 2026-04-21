# File: etl/ai_agent.py
import os
import requests
from dotenv import load_dotenv
from .config import get_supabase_client

load_dotenv()


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
    
    # 3. Gửi yêu cầu qua Gemini API với model discovery để tránh hardcode model chết.
    candidate_models = _build_candidate_models(api_key)
    if not candidate_models:
        return "[AI ERROR] Khong lay duoc danh sach model kha dung tu Gemini ListModels."

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.35}
    }
    headers = {'Content-Type': 'application/json'}

    errors = []
    for model_name in candidate_models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=20)
            if response.status_code == 200:
                result = response.json()
                ai_text = _extract_ai_text(result)
                if ai_text:
                    return f"Goc nhin AI (Trung lap):\n{ai_text}"
                errors.append(f"{model_name}: response has no valid text")
                continue

            # 404 model not found -> try next model
            if response.status_code == 404:
                errors.append(f"{model_name}: 404 model not found")
                continue

            errors.append(f"{model_name}: HTTP {response.status_code} - {response.text[:200]}")
        except Exception as e:
            errors.append(f"{model_name}: connection error - {e}")

    return "[AI ERROR] Khong goi duoc model phu hop. " + " | ".join(errors[:3])
