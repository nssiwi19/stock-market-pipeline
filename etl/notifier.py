import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_msg(message):
    if not TOKEN or not CHAT_ID:
        print("⚠️ Bỏ qua gửi Telegram vì chưa cấu hình TOKEN hoặc CHAT_ID.")
        return False
        
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("📩 Đã gửi thông báo qua Telegram thành công!")
            return True
        else:
            print(f"❌ Lỗi gửi Telegram: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Lỗi kết nối Telegram: {e}")
        return False

if __name__ == "__main__":
    send_telegram_msg("🚀 *Test:* Hệ thống cảnh báo Bot Telegram đã sẵn sàng!")
