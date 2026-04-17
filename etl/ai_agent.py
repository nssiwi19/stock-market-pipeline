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
    
    # 3. Gửi yêu cầu qua Gemini API với fallback models.
    # Có thể override bằng biến GEMINI_MODELS="model-a,model-b"
    models_from_env = os.getenv("GEMINI_MODELS", "").strip()
    if models_from_env:
        candidate_models = [m.strip() for m in models_from_env.split(",") if m.strip()]
    else:
        candidate_models = [
            "gemini-2.0-flash",
            "gemini-1.5-flash",
        ]

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

    return "🤖 *(Lỗi AI: không gọi được model phù hợp. " + " | ".join(errors[:2]) + ")*"
