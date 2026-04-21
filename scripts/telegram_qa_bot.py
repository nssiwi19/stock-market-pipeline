import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# Ensure project root is on sys.path when running as script:
#   python scripts/telegram_qa_bot.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.ai_agent import chatbot_text_to_sql_flow


load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ALLOWED_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
POLL_TIMEOUT = int(os.getenv("TELEGRAM_POLL_TIMEOUT_SECONDS", "30"))
POLL_INTERVAL = float(os.getenv("TELEGRAM_POLL_INTERVAL_SECONDS", "1.0"))
API_TIMEOUT = int(os.getenv("TELEGRAM_API_TIMEOUT_SECONDS", "35"))
AUDIT_PATH = Path(os.getenv("TELEGRAM_QA_AUDIT_PATH", "logs/telegram_qa_audit.jsonl"))
OFFSET_PATH = Path(os.getenv("TELEGRAM_QA_OFFSET_PATH", "logs/telegram_qa_offset.txt"))


def _telegram_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TOKEN}/{method}"


def _load_offset() -> int:
    if not OFFSET_PATH.exists():
        return 0
    try:
        return int(OFFSET_PATH.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def _save_offset(offset: int) -> None:
    OFFSET_PATH.parent.mkdir(parents=True, exist_ok=True)
    OFFSET_PATH.write_text(str(offset), encoding="utf-8")


def _send_message(chat_id: int, text: str) -> None:
    payload = {"chat_id": chat_id, "text": text}
    try:
        resp = requests.post(_telegram_url("sendMessage"), json=payload, timeout=API_TIMEOUT)
        if resp.status_code != 200:
            print(f"[BOT][WARN] sendMessage HTTP {resp.status_code}: {resp.text[:300]}")
    except Exception as exc:
        print(f"[BOT][ERROR] sendMessage failed: {exc}")


def _audit_log(question: str, generated_sql: str, row_count: int, answer: str, success: bool, error: str | None) -> None:
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "question": question,
        "generated_sql": generated_sql,
        "row_count": row_count,
        "answer": answer,
        "success": success,
        "error": error,
    }
    with AUDIT_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _extract_user_text(update: dict) -> tuple[int | None, str | None]:
    message = update.get("message") or update.get("edited_message")
    if not message:
        return None, None
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    text = message.get("text")
    if not text:
        return chat_id, None
    return chat_id, text.strip()


def _is_chat_allowed(chat_id: int) -> bool:
    if not ALLOWED_CHAT_ID:
        return True
    return str(chat_id) == ALLOWED_CHAT_ID


def _build_plain_summary_from_rows(question: str, rows: list[dict]) -> str | None:
    """Sinh câu trả lời dễ hiểu từ kết quả SQL debug."""
    if not rows or not isinstance(rows, list):
        return None
    first = rows[0] if isinstance(rows[0], dict) else {}
    if not first:
        return None

    q = (question or "").lower()
    ticker = first.get("ticker")
    volume = first.get("volume")

    if ticker and volume is not None and any(k in q for k in ["volume", "khoi luong", "thanh khoan"]):
        return f"Mã chứng khoán có volume lớn nhất hôm nay là {ticker} với volume là {volume}."

    # Fallback generic summary
    return f"Kết quả hàng đầu tiên: {first}"


def _handle_text(chat_id: int, text: str) -> None:
    cmd = text.lower().strip()
    if cmd == "/whoami":
        _send_message(chat_id, f"chat_id={chat_id}")
        return
    if cmd in ("/start", "/help"):
        help_text = (
            "Xin chào. Đây là bot Q&A dữ liệu chứng khoán.\n"
            "- Đặt câu hỏi tự nhiên, ví dụ: Volume của mã nào lớn nhất ngày 2026-04-10?\n"
            "- Bot trả lời dựa trên dữ liệu Supabase (Text-to-SQL).\n"
            "- Bot không đưa khuyến nghị mua/bán trực tiếp.\n"
            "- Dùng /sql_only <câu hỏi> để xem SQL debug nhanh."
        )
        _send_message(chat_id, help_text)
        return

    if cmd.startswith("/sql_only"):
        question = text[len("/sql_only"):].strip()
        if not question:
            _send_message(chat_id, "Cách dùng: /sql_only <câu hỏi>")
            return
        _send_message(chat_id, "Đang sinh SQL debug...")
        result = chatbot_text_to_sql_flow(question)
        generated_sql = result.get("sql") or ""
        rows = result.get("rows") or []
        answer = result.get("answer") or "Khong co cau tra loi."
        success = bool(result.get("success"))
        error = result.get("error")
        row_count = len(rows)

        if generated_sql:
            preview = rows[:3] if isinstance(rows, list) else []
            plain_summary = _build_plain_summary_from_rows(question, rows) or "Đã sinh SQL và lấy được dữ liệu."
            _send_message(
                chat_id,
                f"{plain_summary}\n\n[SQL DEBUG]\n{generated_sql}\n\nrow_count={row_count}\npreview={preview}",
            )
        else:
            _send_message(chat_id, f"[SQL DEBUG][ERROR]\n{error or answer}")

        _audit_log(
            question=question,
            generated_sql=generated_sql,
            row_count=row_count,
            answer=answer,
            success=success,
            error=error,
        )
        return

    _send_message(chat_id, "Đang truy vấn dữ liệu, vui lòng đợi...")
    result = chatbot_text_to_sql_flow(text)
    generated_sql = result.get("sql") or ""
    rows = result.get("rows") or []
    answer = result.get("answer") or "Khong co cau tra loi."
    success = bool(result.get("success"))
    error = result.get("error")
    row_count = len(rows)

    # Hide generated_sql from normal user response. Keep SQL only in audit log.
    response_text = f"{answer}\n\n[Audit]\n- row_count: {row_count}"
    _send_message(chat_id, response_text)
    _audit_log(
        question=text,
        generated_sql=generated_sql,
        row_count=row_count,
        answer=answer,
        success=success,
        error=error,
    )


def run_polling() -> None:
    if not TOKEN:
        raise ValueError("Missing TELEGRAM_BOT_TOKEN")

    print("[BOT] Telegram Q&A polling started.")
    offset = _load_offset()
    print(f"[BOT] allowed_chat_id={ALLOWED_CHAT_ID or '(not set)'}")
    print(f"[BOT] start_offset={offset}")
    while True:
        try:
            params = {"timeout": POLL_TIMEOUT}
            if offset > 0:
                params["offset"] = offset
            resp = requests.get(_telegram_url("getUpdates"), params=params, timeout=API_TIMEOUT)
            if resp.status_code != 200:
                print(f"[BOT][WARN] getUpdates HTTP {resp.status_code}: {resp.text[:200]}")
                time.sleep(POLL_INTERVAL)
                continue

            updates = resp.json().get("result", [])
            if updates:
                print(f"[BOT] received {len(updates)} updates")
            for update in updates:
                update_id = int(update.get("update_id", 0))
                if update_id >= offset:
                    offset = update_id + 1
                    _save_offset(offset)

                chat_id, text = _extract_user_text(update)
                if chat_id is None or not text:
                    continue
                print(f"[BOT] update_id={update_id} chat_id={chat_id} text={text[:120]}")
                if not _is_chat_allowed(chat_id):
                    _send_message(chat_id, "Chat ID không được phép sử dụng bot này.")
                    continue
                _handle_text(chat_id, text)

            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("[BOT] Stopped by user.")
            break
        except Exception as exc:
            print(f"[BOT][ERROR] {exc}")
            time.sleep(max(POLL_INTERVAL, 2.0))


if __name__ == "__main__":
    run_polling()
