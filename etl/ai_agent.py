# File: etl/ai_agent.py
import os
import requests
from dotenv import load_dotenv

load_dotenv()

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
    
    # 3. Gửi yêu cầu qua Gemini API (Dùng model Flash siêu tốc độ)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7} # Độ sáng tạo vừa phải
    }
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        if response.status_code == 200:
            result = response.json()
            ai_text = result['candidates'][0]['content']['parts'][0]['text'].strip()
            return f"💡 *Góc nhìn AI:*\n{ai_text}"
        else:
            return f"🤖 *(Lỗi AI sinh văn bản: Cấu trúc trả về không hợp lệ)*"
    except Exception as e:
        return f"🤖 *(Lỗi kết nối AI Server: {e})*"
