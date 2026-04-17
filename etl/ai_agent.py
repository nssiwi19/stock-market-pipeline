# File: etl/ai_agent.py
import os
import requests
from dotenv import load_dotenv

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

def get_ai_market_summary(df_top):
    """Dùng Gemini AI để phân tích và viết nhận định thị trường dựa trên dữ liệu giá."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return "🤖 *(Cảnh báo: Chưa cấu hình GEMINI_API_KEY trong hệ thống để AI nhận định)*"

    print("🧠 AI Agent đang phân tích dữ liệu thị trường...")
    
    # 1. Chuyển đổi dữ liệu Top 5 thành văn bản để mớm cho AI
    data_str = df_top.to_string(index=False)
    
    # 2. Xây dựng System Prompt (Ép AI đóng vai chuyên gia)
    prompt = f"""
    Bạn là một Chuyên gia Phân tích Đầu tư Chứng khoán cao cấp tại Việt Nam.
    Hệ thống tự động của tôi vừa ghi nhận danh sách Top 5 cổ phiếu có khối lượng giao dịch (Volume) khủng nhất thị trường hôm nay:
    
    {data_str}
    
    Nhiệm vụ:
    Viết MỘT đoạn nhận định thị trường (khoảng 3-4 câu) thật chuyên nghiệp, sắc bén bằng tiếng Việt.
    Hãy phân tích xem dòng tiền đang tập trung ở nhóm ngành nào (Ví dụ: Thấy HPG thì nhắc đến Thép, thấy VCB/MBB thì nhắc đến Ngân hàng). 
    Giọng văn năng động, truyền cảm hứng. Không cần lặp lại các con số cụ thể vì tôi đã xem biểu đồ, chỉ cần đưa ra 'Insight' (Góc nhìn).
    """
    
    # 3. Gửi yêu cầu qua Gemini API với model discovery để tránh hardcode model chết.
    candidate_models = _build_candidate_models(api_key)
    if not candidate_models:
        return "🤖 *(Lỗi AI: không lấy được danh sách model khả dụng từ Gemini ListModels.)*"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7} # Độ sáng tạo vừa phải
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
                    return f"💡 *Góc nhìn AI:*\n{ai_text}"
                errors.append(f"{model_name}: response không có text hợp lệ")
                continue

            # 404 model not found -> thử model tiếp theo
            if response.status_code == 404:
                errors.append(f"{model_name}: 404 model not found")
                continue

            errors.append(f"{model_name}: HTTP {response.status_code} - {response.text[:200]}")
        except Exception as e:
            errors.append(f"{model_name}: lỗi kết nối - {e}")

    return "🤖 *(Lỗi AI: không gọi được model phù hợp. " + " | ".join(errors[:3]) + ")*"
