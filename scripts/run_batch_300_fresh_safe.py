import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backfill_financial_reports_from_multi_source import run_pilot


def _load_latest_pilot_tickers(report_path: Path) -> list[str]:
    if not report_path.exists():
        return []
    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    tickers = data.get("pilot_tickers") or []
    if not isinstance(tickers, list):
        return []
    return [str(t).strip().upper() for t in tickers if str(t).strip()]


def main() -> None:
    out_dir = PROJECT_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started = datetime.now()

    report_path = out_dir / "backfill_multisource_pilot_report.json"
    excluded = _load_latest_pilot_tickers(report_path)
    excluded_csv = ",".join(sorted(set(excluded)))

    heartbeat = out_dir / f"batch300_fresh_safe_{run_id}.heartbeat.txt"
    result_file = out_dir / f"batch300_fresh_safe_{run_id}.result.json"
    error_file = out_dir / f"batch300_fresh_safe_{run_id}.error.txt"
    done_file = out_dir / f"batch300_fresh_safe_{run_id}.done.txt"

    heartbeat.write_text(
        f"started_at={started.isoformat()}\nexclude_count={len(set(excluded))}\n",
        encoding="utf-8",
    )
    try:
        result = run_pilot(
            limit=300,
            sleep_ms=0,
            mode="broad",
            min_null_rate=0.0,
            zip_probe_per_ticker=0,
            doc_parse_per_ticker=0,
            doc_top_terms=6,
            doc_page_size=12,
            allow_new_key_insert=True,
            min_new_key_fields=4,
            min_new_key_confidence=0.82,
            new_key_allowed_sources="vietstock_financeinfo",
            allow_ocr_source=False,
            ocr_debug=False,
            ocr_debug_tickers="",
            ocr_debug_max_lines=120,
            ocr_debug_max_files_per_ticker=2,
            ocr_source_tickers="",
            ocr_source_top_missing=0,
            max_workers=2,
            exclude_tickers=excluded_csv,
        )
        payload = {
            "run_id": run_id,
            "started_at": started.isoformat(),
            "elapsed_min": round((datetime.now() - started).total_seconds() / 60.0, 2),
            "exclude_count": len(set(excluded)),
            "excluded_tickers": sorted(set(excluded)),
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
