import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from collectors.research_collector import fetch_research, save_raw
from storage.writer import normalize_and_load
from storage.db import get_db
from loguru import logger

RAW_DIR = Path(__file__).resolve().parent.parent / "backend" / "data" / "raw" / "research"


def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        logger.error(f"No raw files found in {RAW_DIR}. Run without --disk first.")
        sys.exit(1)
    latest = files[-1]
    logger.info(f"Loading from disk: {latest}")
    return json.loads(latest.read_text())


def main():
    parser = argparse.ArgumentParser(description="Load OpenAlex research data into DuckDB")
    parser.add_argument("--disk", action="store_true", help="Load from saved file")
    parser.add_argument("--year", type=int, default=2021, help="Fetch papers from this year (default 2021)")
    parser.add_argument("--max",  type=int, default=100,  help="Max papers per topic (default 100)")
    args = parser.parse_args()

    if args.disk:
        raw = load_from_disk()
    else:
        logger.info(f"Fetching research papers from OpenAlex (since {args.year})...")
        raw = fetch_research(max_per_topic=args.max, min_year=args.year)
        save_raw(raw)

    if not raw:
        logger.warning("No records to load.")
        return

    con = get_db()
    n   = normalize_and_load(raw, "research", con)
    logger.success(f"Done — {n} new research rows inserted into DuckDB.")


if __name__ == "__main__":
    main()