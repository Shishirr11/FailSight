import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from collectors.sam_collector import fetch_sam, save_raw
from storage.writer import normalize_and_load
from storage.db import get_db
from loguru import logger


RAW_DIR = Path(__file__).resolve().parent.parent / "backend" / "data" / "raw" / "sam"


def load_from_disk(filename: str = None) -> list[dict]:
    if filename:
        path = RAW_DIR / filename
        if not path.exists():
            logger.error(f"File not found: {path}")
            sys.exit(1)
    else:
        files = sorted(RAW_DIR.glob("*.json"))
        if not files:
            logger.error(f"No raw files found in {RAW_DIR}. Run without --disk first.")
            sys.exit(1)
        path = files[-1]

    logger.info(f"Loading from disk: {path}")
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description="Load SAM.gov data into DuckDB")
    parser.add_argument("--disk",  action="store_true", help="Load from saved file instead of API")
    parser.add_argument("--file",  type=str,  default=None, help="Specific filename to load (use with --disk)")
    parser.add_argument("--days",  type=int,  default=90,   help="How many days back to fetch (default 90)")
    args = parser.parse_args()

    if args.disk:
        raw = load_from_disk(args.file)
    else:
        logger.info(f"Fetching SAM.gov data (last {args.days} days)...")
        logger.warning("Note: SAM.gov allows 1,000 requests/day. Large fetches may hit the limit.")
        raw = fetch_sam(days_back=args.days)
        save_raw(raw)

    if not raw:
        logger.warning("No records to load.")
        return

    con = get_db()
    n   = normalize_and_load(raw, "sam", con)
    logger.success(f"Done — {n} new SAM rows inserted into DuckDB.")


if __name__ == "__main__":
    main()