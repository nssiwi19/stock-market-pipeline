import os
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

def send_telegram_report_with_chart(df, message):
    """Hàm vẽ biểu đồ Top 5 biến động và gửi ảnh qua Telegram"""
    if not TOKEN or not CHAT_ID:
        print("⚠️ Bỏ qua gửi Telegram vì chưa cấu hình TOKEN hoặc CHAT_ID.")
        return False
        
    try:
        # 1. Tiền xử lý dữ liệu (Lấy 5 mã có khối lượng giao dịch lớn nhất)
        top_5 = df.nlargest(5, 'volume').copy() # Thêm .copy() để tránh warning của Pandas
        
        # KHÔI PHỤC GIÁ TRỊ THỰC: Nhân giá đóng cửa với 1000
        top_5['close_price'] = top_5['close_price'] * 1000 
        
        # 2. Vẽ biểu đồ Bar Chart với Seaborn
        matplotlib.use('Agg') 
        
        # Vẽ Barplot
        ax = sns.barplot(x='ticker', y='close_price', data=top_5, hue='ticker', palette='viridis', legend=False)
        
        plt.title('Top 5 Cổ Phiếu Thanh Khoản Nhất Hôm Nay', fontsize=16, fontweight='bold', pad=20)
        plt.ylabel('Giá Đóng Cửa (VND)', fontsize=12)
        plt.xlabel('Mã Chứng Khoán', fontsize=12)
        
        # Thêm nhãn giá trị trên từng cột
        for p in ax.patches:
            yval = p.get_height()
            ax.annotate(f'{int(yval):,}', 
                        (p.get_x() + p.get_width() / 2., yval), 
                        ha='center', va='bottom', 
                        xytext=(0, 5), 
                        textcoords='offset points',
                        fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        
        # 3. Lưu biểu đồ vào bộ nhớ đệm (không tạo file rác trên ổ cứng)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=150)
        buf.seek(0)
        plt.close()
        
        # 4. Gửi ảnh qua Telegram (sendPhoto)
        url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
        files = {'photo': ('chart.png', buf, 'image/png')}
        data = {
            'chat_id': CHAT_ID, 
            'caption': message, 
            'parse_mode': 'Markdown'
        }
        
        response = requests.post(url, data=data, files=files, timeout=20)
        
        if response.status_code == 200:
            print("🖼️ Đã gửi biểu đồ qua Telegram thành công!")
            return True
        else:
            print(f"❌ Lỗi gửi ảnh Telegram: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Lỗi khi vẽ/gửi biểu đồ: {e}")
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
