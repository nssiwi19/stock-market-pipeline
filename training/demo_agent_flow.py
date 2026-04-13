import json
import os
import requests
from dotenv import load_dotenv

# Load environment variables (Make sure to have GEMINI_API_KEY in your .env)
# Using Gemini here as an example since it has a very accessible free tier API 
# for creating quick agent demos.
load_dotenv()

def fetch_financial_data_mock(ticker):
    """
    Step 1: Using a Toolkit.
    In a real scenario, this would call 'vnstock' to get actual data.
    For this 'Agent Automation' demo, we simulate the tool returning raw JSON data
    extracted from the database or an API.
    """
    print(f"🔧 Agent Tool execution: Fetching raw data for {ticker}...")
    
    # Simulating data that the agent retrieves using a library
    mock_data = {
        "ticker": ticker,
        "company_name": "FPT Corporation",
        "latest_quarter": "Q1-2025",
        "revenue_bn_vnd": 14000,
        "profit_after_tax_bn_vnd": 2000,
        "eps_vnd": 4500,
        "recent_news_headline": "FPT signs major cloud contract with Japanese enterprise, expanding global reach."
    }
    return json.dumps(mock_data)

def ask_llm_to_summarize(raw_data):
    """
    Step 2: The LLM Engine (The "Brain")
    We pass the raw data to the LLM with a specific System Prompt (Persona).
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
         return "❌ Demo Error: GEMINI_API_KEY not found in .env file. \nTo run the demo, set this variable so the 'Agent' can think."

    print("🧠 Agent 'Thinking': Parsing data and writing summary report...")
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    
    prompt = f"""
    Bạn là một Chuyên gia phân tích tài chính AI (AI Financial Analyst).
    Dưới đây là một số dữ liệu thô được hệ thống tự động thu thập về một công ty. 
    Nhiệm vụ của bạn là đọc dữ liệu này và viết một đoạn tóm tắt ngắn gọn (khoảng 3-4 câu) bằng tiếng Việt thật chuyên nghiệp, 
    nhấn mạnh vào Doanh thu, Lợi nhuận và EPS.
    
    Dữ liệu thô (JSON):
    {raw_data}
    """
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}]
    }
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        return result['candidates'][0]['content']['parts'][0]['text']
    except Exception as e:
        return f"Error communicating with AI: {e}"

def run_agent_workflow(ticker):
    """
    Step 3: The Orchestrator
    This is the automated workflow that replaces a human opening dozens of tabs.
    """
    print("====================================")
    print(f"🚀 RUNNING AI AGENT FOR: {ticker}")
    print("====================================\n")
    
    # 1. Agent uses tool
    raw_data = fetch_financial_data_mock(ticker)
    
    # 2. Agent processes information
    summary = ask_llm_to_summarize(raw_data)
    
    # 3. Agent output delivery
    print("\n📩 FINAL REPORT AUTOMATICALLY GENERATED:\n")
    print(summary)
    print("\n====================================")

if __name__ == "__main__":
    # In reality, this could loop through ALL tickers in the database!
    run_agent_workflow("FPT")
