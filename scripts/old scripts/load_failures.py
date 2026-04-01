"""
    python scripts/load_failures.py          # load all sources
    python scripts/load_failures.py --disk   # reload from latest saved JSON
"""
import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from collectors.failure_collector import fetch_failures, save_raw
from storage.writer import normalize_failures, upsert_failures_to_duckdb
from storage.db import get_db
from loguru import logger

RAW_DIR = Path(__file__).resolve().parent.parent / "backend" / "data" / "raw" / "failures"


def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        logger.error("No raw failure files found. Run without --disk first.")
        sys.exit(1)
    latest = files[-1]
    logger.info(f"Loading from disk: {latest}")
    return json.loads(latest.read_text())


def main():
    parser = argparse.ArgumentParser(description="Load failure data into DuckDB")
    parser.add_argument("--disk",       action="store_true")
    parser.add_argument("--no-failory", action="store_true")
    parser.add_argument("--no-deep",    action="store_true")
    args = parser.parse_args()

    if args.disk:
        raw = load_from_disk()
    else:
        include_failory = not args.no_failory
        deep            = not args.no_deep
        raw = fetch_failures(include_failory=include_failory, deep=deep)
        save_raw(raw)

    if not raw:
        logger.warning("No failure records to load.")
        return

    df  = normalize_failures(raw)
    con = get_db()
    n   = upsert_failures_to_duckdb(df, con)
    logger.success(f"Done — {n} new failure records inserted into DuckDB.")

    rows = con.execute("""
        SELECT sector, COUNT(*) as count
        FROM failures_unified
        GROUP BY sector ORDER BY count DESC LIMIT 8
    """).fetchall()
    logger.info("Failures by sector:")
    for sector, count in rows:
        logger.info(f"  {sector:<35} {count} failures")


if __name__ == "__main__":
    main()