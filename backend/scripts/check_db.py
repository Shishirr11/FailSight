# Row counts per source
# Top sectors
# Most recent record per source
# Total failures loaded


import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from storage.db import get_db
from loguru import logger


def main():
    con = get_db()

    total = con.execute(
        "SELECT COUNT(*) FROM unified_opportunities"
    ).fetchone()[0]
    logger.info(f"Total opportunities: {total}")

    by_source = con.execute("""
        SELECT source, COUNT(*) as count
        FROM unified_opportunities
        GROUP BY source
        ORDER BY count DESC
    """).fetchall()

    logger.info("Rows per source:")
    for source, count in by_source:
        logger.info(f"  {source:<12} {count:>6} rows")

    logger.info("Most recent record per source:")
    recent = con.execute("""
        SELECT source, MAX(posted_date)::VARCHAR as latest
        FROM unified_opportunities
        GROUP BY source
    """).fetchall()
    for source, latest in recent:
        logger.info(f"  {source:<12} {latest}")

    logger.info("Top 10 sectors:")
    sectors = con.execute("""
        SELECT sector, COUNT(*) as count
        FROM unified_opportunities
        WHERE sector IS NOT NULL
        GROUP BY sector
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    for sector, count in sectors:
        logger.info(f"  {sector:<35} {count:>5} rows")

    failures = con.execute(
        "SELECT COUNT(*) FROM failures_unified"
    ).fetchone()[0]
    logger.info(f"Known failures loaded: {failures}")

    raw_root = Path(__file__).resolve().parent.parent / "backend" / "data" / "raw"
    logger.info("Raw files on disk:")
    for source_dir in sorted(raw_root.iterdir()):
        if source_dir.is_dir():
            files = list(source_dir.glob("*.json"))
            logger.info(f"  {source_dir.name:<12} {len(files)} file(s)")


if __name__ == "__main__":
    main()