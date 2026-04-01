import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from storage.writer import normalize_and_load
from storage.db import get_db
from loguru import logger


RAW_ROOT = Path(__file__).resolve().parent.parent / "backend" / "data" / "raw"
SOURCES  = ["sam", "grants", "patents", "research", "failures"]


def load_source(source: str, filename: str = None) -> int:
    source_dir = RAW_ROOT / source
    if not source_dir.exists():
        logger.warning(f"No raw directory for source '{source}' — skipping.")
        return 0

    if filename:
        path = source_dir / filename
        if not path.exists():
            logger.error(f"File not found: {path}")
            return 0
    else:
        files = sorted(source_dir.glob("*.json"))
        if not files:
            logger.warning(f"No raw files found for source '{source}' — skipping.")
            return 0
        path = files[-1]

    logger.info(f"Loading {source} from: {path.name}")
    raw = json.loads(path.read_text())
    logger.info(f"  {len(raw)} records read from disk")

    con = get_db()
    n   = normalize_and_load(raw, source, con)
    logger.success(f"  {n} new rows inserted for source '{source}'")
    return n


def main():
    parser = argparse.ArgumentParser(description="Load raw JSON files into DuckDB")
    parser.add_argument("--source", type=str, choices=SOURCES)
    parser.add_argument("--file",   type=str, default=None)
    parser.add_argument("--all",    action="store_true")
    args = parser.parse_args()

    if not args.source and not args.all:
        parser.print_help()
        sys.exit(1)

    total = 0

    if args.all:
        for source in SOURCES:
            total += load_source(source)
    else:
        total = load_source(args.source, args.file)

    logger.success(f"Total new rows inserted: {total}")


if __name__ == "__main__":
    main()