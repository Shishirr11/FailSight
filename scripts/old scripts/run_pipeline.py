import sys
import json
import uuid
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from storage.db import get_db
from storage.schema import create_schema
from storage.writer import normalize_and_load, normalize_failures, upsert_failures_to_duckdb
from collectors.failure_collector import fetch_failures, save_raw
from loguru import logger

RAW_ROOT = Path(__file__).resolve().parent.parent / "backend" / "data" / "raw"



def log_run(con, source: str, rows_added: int, status: str,
            started_at: datetime, error_msg: str = "") -> None:
    con.execute("""
        INSERT INTO pipeline_log (run_id, started_at, finished_at, source, rows_added, status, error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        str(uuid.uuid4()),
        started_at.isoformat(),
        datetime.now().isoformat(),
        source,
        rows_added,
        status,
        error_msg,
    ])


def run_grants(con, from_disk: bool = False) -> int:
    from collectors.grants_collector import fetch_grants, save_raw
    started = datetime.now()
    try:
        if from_disk:
            files = sorted((RAW_ROOT / "grants").glob("*.json"))
            if not files:
                logger.warning("No grants files on disk — skipping.")
                return 0
            raw = json.loads(files[-1].read_text())
            logger.info(f"Grants: loaded {len(raw)} records from disk ({files[-1].name})")
        else:
            logger.info("Grants: fetching from Grants.gov API...")
            raw = fetch_grants()
            save_raw(raw)
        n = normalize_and_load(raw, "grants", con)
        log_run(con, "grants", n, "ok", started)
        return n
    except Exception as e:
        logger.error(f"Grants pipeline failed: {e}")
        log_run(con, "grants", 0, "error", started, str(e))
        return 0


def run_sam(con, from_disk: bool = False) -> int:
    from collectors.sam_collector import fetch_sam, save_raw
    started = datetime.now()
    try:
        if from_disk:
            files = sorted((RAW_ROOT / "sam").glob("*.json"))
            if not files:
                logger.warning("No SAM files on disk — skipping.")
                return 0
            raw = json.loads(files[-1].read_text())
            logger.info(f"SAM: loaded {len(raw)} records from disk ({files[-1].name})")
        else:
            logger.info("SAM: fetching from SAM.gov API...")
            raw = fetch_sam(days_back=1)
            save_raw(raw)
        n = normalize_and_load(raw, "sam", con)
        log_run(con, "sam", n, "ok", started)
        return n
    except Exception as e:
        logger.error(f"SAM pipeline failed: {e}")
        log_run(con, "sam", 0, "error", started, str(e))
        return 0


def run_research(con, from_disk: bool = False) -> int:
    from collectors.research_collector import fetch_research, save_raw
    started = datetime.now()
    try:
        if from_disk:
            files = sorted((RAW_ROOT / "research").glob("*.json"))
            if not files:
                logger.warning("No research files on disk — skipping.")
                return 0
            raw = json.loads(files[-1].read_text())
            logger.info(f"Research: loaded {len(raw)} records from disk ({files[-1].name})")
        else:
            logger.info("Research: fetching from OpenAlex API...")
            raw = fetch_research()
            save_raw(raw)
        n = normalize_and_load(raw, "research", con)
        log_run(con, "research", n, "ok", started)
        return n
    except Exception as e:
        logger.error(f"Research pipeline failed: {e}")
        log_run(con, "research", 0, "error", started, str(e))
        return 0


def run_patents(con, from_disk: bool = False) -> int:
    from collectors.patents_collector import fetch_patents, save_raw
    started = datetime.now()
    try:
        if from_disk:
            files = sorted((RAW_ROOT / "patents").glob("*.json"))
            if not files:
                logger.warning("No patents files on disk — PatentsView API still migrating.")
                return 0
            raw = json.loads(files[-1].read_text())
            logger.info(f"Patents: loaded {len(raw)} records from disk ({files[-1].name})")
        else:
            logger.info("Patents: fetching from PatentsView API...")
            raw = fetch_patents()
            if not raw:
                logger.warning("Patents: no data returned — API may still be migrating.")
                return 0
            save_raw(raw)
        n = normalize_and_load(raw, "patents", con)
        log_run(con, "patents", n, "ok", started)
        return n
    except Exception as e:
        logger.error(f"Patents pipeline failed: {e}")
        log_run(con, "patents", 0, "error", started, str(e))
        return 0


def run_failures(con, from_disk: bool = False, skip: bool = False) -> int:
    if skip:
        logger.info("Failures: skipped (--skip-failures flag).")
        return 0
    started = datetime.now()
    try:
        if from_disk:
            files = sorted((RAW_ROOT / "failures").glob("*.json"))
            if not files:
                logger.warning("No failures files on disk — skipping.")
                return 0
            raw = json.loads(files[-1].read_text())
            logger.info(f"Failures: loaded {len(raw)} records from disk ({files[-1].name})")
        else:
            logger.info("Failures: fetching from CB Insights + Failory (~2 min)...")
            raw = fetch_failures(include_failory=True, deep=True)
            save_raw(raw)
        df = normalize_failures(raw)
        n  = upsert_failures_to_duckdb(df, con)
        log_run(con, "failures", n, "ok", started)
        return n
    except Exception as e:
        logger.error(f"Failures pipeline failed: {e}")
        log_run(con, "failures", 0, "error", started, str(e))
        return 0

RUNNERS = {
    "grants":   run_grants,
    # "sam":      run_sam,
    "research": run_research,
    "patents":  run_patents,
    "failures": run_failures,
}


def main():
    parser = argparse.ArgumentParser(description="FOIP daily data refresh pipeline")
    parser.add_argument("--source",        type=str, default=None,
                        choices=list(RUNNERS.keys()))
    parser.add_argument("--disk",          action="store_true")
    parser.add_argument("--skip-failures", action="store_true")
    args = parser.parse_args()

    pipeline_start = datetime.now()
    logger.info("=" * 60)
    logger.info(f"FOIP Pipeline starting — {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Mode: {'disk' if args.disk else 'live API'}")
    logger.info("=" * 60)

    con = get_db()
    create_schema(con)

    before          = con.execute("SELECT COUNT(*) FROM unified_opportunities").fetchone()[0]
    before_failures = con.execute("SELECT COUNT(*) FROM failures_unified").fetchone()[0]

    sources_to_run = [args.source] if args.source else list(RUNNERS.keys())
    total_new = 0

    for source in sources_to_run:
        logger.info(f"\n── Running: {source.upper()} ──")
        runner = RUNNERS[source]
        if source == "failures":
            n = runner(con, from_disk=args.disk, skip=args.skip_failures)
        else:
            n = runner(con, from_disk=args.disk)
        total_new += n
        logger.info(f"   {source}: +{n} new rows")

    after          = con.execute("SELECT COUNT(*) FROM unified_opportunities").fetchone()[0]
    after_failures = con.execute("SELECT COUNT(*) FROM failures_unified").fetchone()[0]

    duration = (datetime.now() - pipeline_start).total_seconds()
    logger.info("\n" + "=" * 60)
    logger.success(f"Pipeline complete in {duration:.1f}s")
    logger.info(f"Opportunities: {before:,} → {after:,} (+{after - before} new)")
    logger.info(f"Failures:      {before_failures:,} → {after_failures:,} (+{after_failures - before_failures} new)")
    logger.info("=" * 60)

    recent = con.execute("""
        SELECT source, rows_added, status, started_at
        FROM pipeline_log
        ORDER BY started_at DESC
        LIMIT 12
    """).fetchall()

    logger.info("\nRecent pipeline runs:")
    for row in recent:
        icon = "\/" if row[2] == "ok" else "X"
        logger.info(f"  {icon} {row[0]:<12} +{row[1]} rows   {str(row[3])[:19]}")


if __name__ == "__main__":
    main()

"""
    python scripts/run_pipeline.py              
    python scripts/run_pipeline.py --source sam 
    python scripts/run_pipeline.py --skip-failures
"""