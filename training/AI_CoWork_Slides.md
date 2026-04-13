---
marp: true
theme: default
class: lead
paginate: true
backgroundColor: #f4f6f8
style: |
  h1 { color: #1e3a8a; }
  h2 { color: #2563eb; }
  .highlight { color: #e11d48; font-weight: bold; }
---

# E15: AI COLLABORATION & AUTOMATION
## 🚀 Trở thành "Tech Evangelist" trong kỷ nguyên AI
**Mục tiêu:** Không chỉ dùng AI để "hỏi-đáp", hãy biến AI thành đồng sự (Co-Worker) và tự động hóa các tác vụ lặp lại!

---

# Tư duy Cộng tác AI (AI Co-Working)
Thay vì nghĩ "AI là Google cải tiến", hãy xem AI như:
- 🧑‍💻 **Junior Developer / Data Analyst:** Sẵn sàng viết script, xử lý dữ liệu.
- 💡 **Brainstorming Partner:** Đánh giá logic, tìm lỗ hổng trong quy trình.
- ⚙️ **Automation Engine:** Chạy các workflow không cần sự can thiệp của con người.

> *Từ "Prompt Engineering" (Viết lệnh) sang "Context Provisioning" (Cấp bối cảnh).*

---

# 1. Claude Co-work & Artifacts
## Ứng dụng "Artifacts" để phát triển siêu tốc

**Tính năng Artifacts là gì?**
- Là một cửa sổ độc lập (bên cạnh khung chat) hiển thị trực tiếp code, tài liệu markdown, biểu đồ (Mermaid), và thậm chí cả giao diện React (UI) hoặc HTML/CSS.
- **Lợi ích:** Bạn không cần copy/paste code vào Editor để xem thử. Mọi thứ được *Visualize* lập tức!

---

# Kỹ thuật Prompting cho Logic Phức tạp

Để AI hiểu dự án (ví dụ MCNA), đừng chỉ đưa yêu cầu. Hãy dùng **Cấu trúc 3 lớp**:

1. **Role & Context (Định vị):** *"Đóng vai Senior System Architect. Dự án đang sử dụng Supabase và Python cho Time-series data."*
2. **Logic & Constraints (Ràng buộc):** *"Chỉ lấy giá đóng cửa điều chỉnh (Adjusted Close). Nếu API rỗng, log lỗi thay vì dừng chương trình."*
3. **Format & Output (Đầu ra):** *"Xuất cấu trúc thư mục ra bảng Markdown và tạo file Artifact cho từng file code."*

---

# 2. Thay thế thao tác thủ công bằng AI Agents

**Perplexity & Computer Use (Agentic AI):**
Sự khác biệt của Agent AI là khả năng **tự lên kế hoạch (Planning)**, **dùng công cụ (Tools)**, và **tự sửa lỗi (Self-Correction)**.

- **Duyệt web chuyên sâu:** Perplexity tự đào sâu các báo cáo tài chính thay vì bạn phải lướt từng trang cafef/vietstock.
- **Action (Hành động):** Tương lai gần, AI (như Claude Computer Use) có thể trực tiếp click xuất file Excel báo cáo từ hệ thống ERP công ty.

---

# 3. Use-case Thực tế (Demo)
## The Automated Financial Researcher

Thay vì mỗi sáng tìm tin Doanh nghiệp/Thị trường:
1. Script đọc danh sách Mã Chứng Khoán từ Database.
2. Script đẩy tín hiệu vào AI (qua API).
3. AI tự đi "đọc" báo cáo và tin tức thị trường mới nhất.
4. AI tóm tắt các điểm nhấn: <span class="highlight">Revenue, Profit, EPS</span>.
5. Gửi báo cáo Summary thẳng vào Email / Telegram của bạn.

**(Xem Demo Tool trong thư mục /training)**

---

# 🎯 Hành động ngay hôm nay!
1. Kích hoạt tính năng **Artifacts** trên Claude (nếu chưa có).
2. Khi gặp một file Excel cần xử lý dài dòng -> Ném vào AI và yêu cầu viết Script tự động Python.
3. Không chấp nhận Manual Work - **Think Automation First!**
