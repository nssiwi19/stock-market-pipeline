# Sử dụng Python 3.10 slim để môi trường nhẹ gọn
FROM python:3.10-slim

# Cài đặt các thư viện hệ thống cần thiết (nếu có)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập thư mục làm việc
WORKDIR /app

# Sao chép các file cấu hình môi trường và thư viện
COPY requirements.txt .

# Cài đặt thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn vào image
COPY . .

# Thiết lập biến môi trường
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Lệnh chạy bot mặc định
CMD ["python", "scripts/telegram_qa_bot.py"]
