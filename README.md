# 📈 Vietnam Stock Market Pipeline

Automated ETL pipeline that collects Vietnamese stock market data daily and delivers insights via a Telegram AI bot and Looker Studio dashboards.

![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Pipeline](https://img.shields.io/badge/schedule-weekdays%2017%3A00%20VN-orange)

## What It Does

| Step | Source | Output |
|---|---|---|
| **1. Extract Tickers** | vnstock3 (KBS) | `tickers` — ~1 700 mã HOSE/HNX/UPCOM |
| **2. Enrich Company Info** | vnstock3 + Company API | industry, company_name, contact_phone |
| **3. Extract Daily Prices** | KBS REST API (10 threads) | `daily_prices` — OHLCV 5 days rolling |
| **4. Extract Financials** | CafeF HTML scraping (10 threads) | `financial_reports` — 50+ metrics × 3 years |

After ETL completes, an **AI agent (Gemini)** writes a neutral market summary and sends it with a chart to Telegram.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Data Sources    │     │   ETL Pipeline   │     │    Supabase (PG)    │
│  · vnstock3/KBS  │────▷│  run_pipeline.py  │────▷│  tickers            │
│  · CafeF HTML    │     │  (GitHub Actions) │     │  daily_prices       │
└─────────────────┘     └──────────────────┘     │  financial_reports  │
                                                  └────────┬────────────┘
                                                           │
                              ┌─────────────────────────────┤
                              ▼                             ▼
                   ┌──────────────────┐          ┌──────────────────┐
                   │  Telegram Q&A Bot │          │  BI Gold Tables  │
                   │  (Text-to-SQL +   │          │  (Looker Studio) │
                   │   Gemini AI)      │          └──────────────────┘
                   └──────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- [Supabase](https://supabase.com) project (free tier works)
- Telegram bot token (via [@BotFather](https://t.me/BotFather))
- Gemini API key (via [Google AI Studio](https://aistudio.google.com))

### 1. Clone & Install

```bash
git clone https://github.com/<your-org>/stock-market-pipeline.git
cd stock-market-pipeline
python -m venv .venv && .venv/Scripts/activate   # Windows
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.template .env
# Edit .env with your credentials:
#   SUPABASE_URL, SUPABASE_KEY,
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
#   GEMINI_API_KEY
```

### 3. Initialize Database

Run these SQL files in your Supabase SQL Editor (in order):

```
scripts/00_init_database.sql
scripts/04_create_execute_readonly_sql_rpc.sql   # Required for Telegram bot
scripts/08_create_bi_gold_table.sql              # Optional: BI gold layer
```

### 4. Run the Pipeline

```bash
python run_pipeline.py
```

### 5. Start the Telegram Bot

```bash
python scripts/telegram_qa_bot.py
```

**Bot commands:**

| Command | Description |
|---|---|
| `/help` | Show usage guide |
| `/report` | Get latest market summary + chart |
| `/sql_only <question>` | Debug mode — shows generated SQL |
| Free text | Natural language Q&A (Text-to-SQL) |

## Database Schema

### `tickers`

Primary key: `ticker`. Stores company profile metadata (exchange, industry, company_name).

### `daily_prices`

Unique key: `(ticker, trading_date)`. OHLCV data with a `CHECK` constraint ensuring `low ≤ open/close ≤ high` and `volume ≥ 0`.

### `financial_reports`

Unique key: `(ticker, report_type, period)`. 50+ financial metrics covering Income Statement, Balance Sheet, Cash Flow, and derived ratios (ROE, ROA, margins, D/E).

> **Unit convention:** monetary values in `tỷ VND` (divided by 1e9 in ETL). Ratios stored as decimals (0.15 = 15%). See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for full details.

## BI Gold Layer

For dashboards, use the pre-cleaned materialized tables instead of raw `financial_reports`:

```sql
-- Refresh gold tables
SELECT public.refresh_financial_reports_bi_gold();

-- Top industries by revenue
SELECT industry_normalized, ticker_count, total_value_bn
FROM public.financial_reports_bi_gold_industry_agg
WHERE metric = 'revenue' AND min_ticker_gate = true
ORDER BY total_value_bn DESC;
```

## Deployment

The pipeline runs automatically on **weekdays at 17:00 VN time** via GitHub Actions.

The Telegram bot can be deployed as a long-running worker:

| Platform | Config |
|---|---|
| **GitHub Actions** | `.github/workflows/daily_stock_pipeline.yml` |
| **Fly.io** | `fly.toml` — Singapore region |
| **Render** | `render.yaml` — Worker type, free plan |
| **Docker** | `docker build -t stock-bot . && docker run --env-file .env stock-bot` |

## Testing

```bash
python -m pytest tests/ -v
```

## Project Structure

```
├── run_pipeline.py              # Main ETL orchestrator
├── etl/
│   ├── config.py                # Supabase connection
│   ├── extract_tickers.py       # Step 1: vnstock3 listing
│   ├── populate_company_info.py # Step 2: company enrichment
│   ├── extract_daily_prices.py  # Step 3: KBS API prices
│   ├── extract_financials.py    # Step 4: CafeF scraping
│   ├── ai_agent.py              # Gemini Text-to-SQL + market summary
│   └── notifier.py              # Telegram messaging + charts
├── scripts/                     # SQL migrations & one-off scripts
├── tests/                       # Unit tests
├── database/                    # Base schema reference
├── training/                    # Internal workshop materials
├── .github/workflows/           # CI/CD pipelines
├── Dockerfile                   # Container for Telegram bot
├── requirements.txt             # Pinned Python dependencies
└── DATA_DICTIONARY.md           # Column definitions & units
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). TL;DR: feature branches → PR → review → merge.

## License

MIT
