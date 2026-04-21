
# 📈 Vietnam Stock Market Data Pipeline

An automated, cloud-based Data Engineering pipeline that extracts daily trading data of 700+ tickers from the Vietnam Stock Exchange (HOSE & HNX) and loads it into a Data Warehouse for visualization.

**Author:** Lê Viết Đăng
**Status:** Completed & Actively Running

## 🏗️ Architecture

The system follows a modern ELT (Extract, Load, Transform) architecture, fully automated via GitHub Actions and hosted on Supabase (PostgreSQL).

```mermaid
graph TD;
    A[HOSE/HNX Market] -->|API| B(Python ETL Scripts);
    B -->|Upsert via API| C[(Supabase PostgreSQL)];
    D[GitHub Actions] -->|Trigger Daily at 18:00| B;
    B -->|Success/Fail Alert| E[Telegram Bot];
    C -->|DirectQuery| F[Power BI Dashboard];
    
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#00bfff,stroke:#333,stroke-width:2px
    style D fill:#fbbf24,stroke:#333,stroke-width:2px

## Telegram Q&A Bot (Text-to-SQL)

Run interactive Q&A bot (polling mode):

```bash
python scripts/telegram_qa_bot.py
```

Required setup:
- Apply `scripts/04_create_execute_readonly_sql_rpc.sql` on Supabase.
- Configure `.env` keys:
  - `TELEGRAM_BOT_TOKEN`
  - `TELEGRAM_CHAT_ID` (optional allow-list, recommended)
  - `GEMINI_API_KEY`

Audit log:
- Bot writes per-question audit to `logs/telegram_qa_audit.jsonl` with:
  - `question`
  - `generated_sql`
  - `row_count`
  - `answer`
