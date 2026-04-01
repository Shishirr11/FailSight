import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from collectors.grants_collector import fetch_grants, save_raw
from storage.writer import normalize_and_load
from storage.db import get_db
from loguru import logger


RAW_DIR = Path(__file__).resolve().parent.parent / "backend" / "data" / "raw" / "grants"


def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        logger.error(f"No raw files found in {RAW_DIR}. Run without --disk first.")
        sys.exit(1)
    latest = files[-1]
    logger.info(f"Loading from disk: {latest}")
    return json.loads(latest.read_text())


def main():
    use_disk = "--disk" in sys.argv

    if use_disk:
        raw = load_from_disk()
    else:
        logger.info("Fetching fresh data from Grants.gov...")
        raw = fetch_grants()
        save_raw(raw)

    if not raw:
        logger.warning("No records to load.")
        return

    con = get_db()
    n   = normalize_and_load(raw, "grants", con)
    logger.success(f"Done — {n} new grants rows inserted into DuckDB.")


if __name__ == "__main__":
    main()