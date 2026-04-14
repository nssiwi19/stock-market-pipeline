import requests
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import io
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

def send_telegram_report_with_chart(df_top, summary_text):
    """
    Vẽ biểu đồ và gửi kèm ảnh qua Telegram
    df_top: DataFrame chứa 'ticker' và 'price_change'
    summary_text: Nội dung caption
    """
    if not TOKEN or not CHAT_ID:
        print("⚠️ Bỏ qua gửi Telegram Chart vì chưa cấu hình TOKEN hoặc CHAT_ID.")
        return False

    try:
        # 1. Cấu hình Matplotlib Backend (không dùng giao diện)
        matplotlib.use('Agg')
        sns.set_theme(style="whitegrid")
        
        # 2. Vẽ biểu đồ
        plt.figure(figsize=(10, 6))
        colors = ['#2ecc71' if x > 0 else '#e74c3c' for x in df_top['price_change']]
        
        ax = sns.barplot(x='ticker', y='price_change', data=df_top, palette=colors, hue='ticker', legend=False)
        
        plt.title("Top 5 Biến động mạnh nhất phiên", fontsize=16, fontweight='bold', pad=20)
        plt.ylabel("% Thay đổi", fontsize=12)
        plt.xlabel("Mã chứng khoán", fontsize=12)
        
        # Thêm nhãn giá trị trên đầu cột
        for p in ax.patches:
            ax.annotate(f"{p.get_height():.2f}%", 
                        (p.get_x() + p.get_width() / 2., p.get_height()), 
                        ha='center', va='center', 
                        xytext=(0, 9), 
                        textcoords='offset points',
                        fontsize=10, fontweight='bold')

        plt.tight_layout()

        # 3. Chuyển biểu đồ vào bộ nhớ đệm (Buffer)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=300)
        buf.seek(0)
        plt.close()

        # 4. Gửi qua Telegram API (sendPhoto)
        url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
        files = {'photo': ('chart.png', buf, 'image/png')}
        data = {
            'chat_id': CHAT_ID, 
            'caption': summary_text, 
            'parse_mode': 'Markdown'
        }
        
        response = requests.post(url, data=data, files=files, timeout=20)
        
        if response.status_code == 200:
            print("📸 Đã gửi báo cáo kèm biểu đồ thành công!")
            return True
        else:
            print(f"❌ Lỗi gửi Telegram Chart: {response.text}")
            return False

    except Exception as e:
        print(f"❌ Lỗi xử lý biểu đồ/Telegram: {e}")
        return False

if __name__ == "__main__":
    # Test message
    send_telegram_msg("🚀 *Test:* Hệ thống cảnh báo Bot Telegram đã sẵn sàng!")
    
    # Test chart (dummy data)
    try:
        import pandas as pd
        dummy_data = pd.DataFrame({
            'ticker': ['VCB', 'FPT', 'VIC', 'VHM', 'HPG'],
            'price_change': [2.5, -1.2, 0.8, -3.4, 4.1]
        })
        send_telegram_report_with_chart(dummy_data, "📊 *Báo cáo thử nghiệm biểu đồ*")
    except ImportError:
        print("Cần cài đặt pandas để chạy test chart.")
