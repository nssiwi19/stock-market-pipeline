import json
import sys
import time
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backfill_financial_reports_from_multi_source import run_pilot


def main() -> None:
    started = datetime.now()
    out_dir = Path("scripts/output")
    out_dir.mkdir(parents=True, exist_ok=True)
    heartbeat = out_dir / "backfill_batch1_heartbeat.txt"
    heartbeat.write_text(f"started_at={started.isoformat()}\n", encoding="utf-8")

    out = out_dir / "backfill_batch1_result.json"
    err_out = out_dir / "backfill_batch1_error.txt"
    try:
        last_exc = None
        result = None
        for attempt in range(1, 4):
            try:
                result = run_pilot(
                    limit=300,
                    sleep_ms=0,
                    mode="targeted",
                    min_null_rate=0.0,
                    zip_probe_per_ticker=0,
                    doc_parse_per_ticker=0,
                    doc_top_terms=1,
                    doc_page_size=5,
                )
                break
            except Exception as exc:
                last_exc = exc
                if attempt >= 3:
                    raise
                time.sleep(3 * attempt)
        if result is None and last_exc is not None:
            raise last_exc
        elapsed_min = round((datetime.now() - started).total_seconds() / 60.0, 2)
        payload = {
            "batch": 1,
            "started_at": started.isoformat(),
            "elapsed_min": elapsed_min,
            "result": result,
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    except Exception as exc:
        err_out.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
        raise


if __name__ == "__main__":
    main()
