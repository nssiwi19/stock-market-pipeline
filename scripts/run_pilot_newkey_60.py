import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backfill_financial_reports_from_multi_source import run_pilot


def main() -> None:
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started = datetime.now()

    heartbeat = out_dir / f"newkey_balanced_60_{run_id}.heartbeat.txt"
    result_file = out_dir / f"newkey_balanced_60_{run_id}.result.json"
    error_file = out_dir / f"newkey_balanced_60_{run_id}.error.txt"
    done_file = out_dir / f"newkey_balanced_60_{run_id}.done.txt"

    heartbeat.write_text(f"started_at={started.isoformat()}\n", encoding="utf-8")
    try:
        result = run_pilot(
            limit=60,
            sleep_ms=0,
            mode="targeted",
            min_null_rate=0.4,
            zip_probe_per_ticker=1,
            doc_parse_per_ticker=2,
            doc_top_terms=6,
            doc_page_size=12,
            allow_new_key_insert=True,
            min_new_key_fields=4,
            min_new_key_confidence=0.8,
            new_key_allowed_sources="vietstock_bctc_documents,vietstock_financeinfo",
        )
        payload = {
            "run_id": run_id,
            "started_at": started.isoformat(),
            "elapsed_min": round((datetime.now() - started).total_seconds() / 60.0, 2),
            "result": result,
        }
        result_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(result_file)
    except BaseException as exc:
        error_file.write_text(
            f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}",
            encoding="utf-8",
        )
        raise
    finally:
        done_file.write_text(datetime.now().isoformat(), encoding="utf-8")


if __name__ == "__main__":
    main()
