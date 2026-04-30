import sys
import uuid
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

from storage.db import get_db
from storage.schema import create_schema
from storage.writer import write_records
from loguru import logger


def _log_run(con, source: str, rows: int, status: str,
             started: datetime, error: str = "") -> None:
    con.execute("""
        INSERT OR REPLACE INTO pipeline_log
            (run_id, started_at, finished_at, source, rows_added, status, error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        str(uuid.uuid4()), started.isoformat(), datetime.now().isoformat(),
        source, rows, status, error,
    ])


def run_source(
    source: str,
    con,
    from_disk:    bool = False,
    fetch_detail: bool = False,
    **kwargs,
) -> int:
    started = datetime.now()
    logger.info(f"{'─'*50}")
    logger.info(f"Starting: {source.upper()}")
    logger.info(f"{'─'*50}")

    try:
        if source == "grants":
            from collectors.grants_collector import fetch_grants, load_from_disk, save_raw
            records = load_from_disk() if from_disk else fetch_grants(fetch_detail=fetch_detail)
            if not from_disk and records:
                save_raw(records)

        elif source == "sam":
            from collectors.sam_collector import fetch_sam, load_from_disk, save_raw
            if from_disk:
                records = load_from_disk()
            else:
                records = fetch_sam(
                    days_back    = kwargs.get("days_back", 90),
                    fetch_detail = fetch_detail,
                )
                if records:
                    save_raw(records)

        elif source == "sbir":
            from collectors.sbir_collector import fetch_sbir, load_from_disk, save_raw
            records = load_from_disk() if from_disk else fetch_sbir()
            if not from_disk and records:
                save_raw(records)

        elif source == "nsf":
            from collectors.nsf_collector import fetch_nsf, load_from_disk, save_raw
            records = load_from_disk() if from_disk else fetch_nsf()
            if not from_disk and records:
                save_raw(records)

        elif source == "research":
            from collectors.research_collector import fetch_research, load_from_disk, save_raw
            records = load_from_disk() if from_disk else fetch_research()
            if not from_disk and records:
                save_raw(records)

        elif source == "patents":
            from collectors.patents_collector import fetch_patents, load_from_disk, save_raw
            if from_disk:
                try:
                    records = load_from_disk()
                except FileNotFoundError:
                    logger.warning("Patents: no files on disk — skipping.")
                    return 0
            else:
                records = fetch_patents()
                if not records:
                    logger.warning("Patents: no data returned — API may still be migrating.")
                    _log_run(con, source, 0, "skipped", started)
                    return 0
                save_raw(records)

        elif source == "failures":
            from collectors.failure_collector import fetch_failures, load_from_disk, save_raw
            records = load_from_disk() if from_disk else fetch_failures(
                include_failory = True,
                deep            = not kwargs.get("no_deep", False),
            )
            if not from_disk and records:
                save_raw(records)

        else:
            logger.error(f"Unknown source: {source}")
            return 0

        if not records:
            logger.warning(f"{source}: no records returned.")
            _log_run(con, source, 0, "empty", started)
            return 0

        n = write_records(records, source, con)
        _log_run(con, source, n, "ok", started)
        return n

    except Exception as e:
        logger.error(f"{source} failed: {e}")
        import traceback; traceback.print_exc()
        _log_run(con, source, 0, "error", started, str(e))
        return 0


ALL_SOURCES = ["grants", "sam", "sbir", "nsf", "research", "patents", "failures"]


def main():
    parser = argparse.ArgumentParser(
        description="Findout data ingest pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source",        choices=ALL_SOURCES, default=None)
    parser.add_argument("--disk",          action="store_true")
    parser.add_argument("--no-detail",     action="store_true")
    parser.add_argument("--skip-failures", action="store_true")
    parser.add_argument("--no-deep",       action="store_true")
    parser.add_argument("--days",          type=int, default=90)
    parser.add_argument("--build-index",   action="store_true")
    args = parser.parse_args()

    start_time = datetime.now()
    con = get_db()
    create_schema(con)

    sources = [args.source] if args.source else ALL_SOURCES
    if args.skip_failures and "failures" in sources:
        sources.remove("failures")

    logger.info(f"Ingest starting — sources: {sources}")
    logger.info(f"Options: disk={args.disk} | detail={not args.no_detail} | "
                f"deep={not args.no_deep} | days={args.days}")

    results: dict[str, int] = {}
    for source in sources:
        n = run_source(
            source       = source,
            con          = con,
            from_disk    = args.disk,
            fetch_detail = not args.no_detail,
            days_back    = args.days,
            no_deep      = args.no_deep,
        )
        results[source] = n

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"\n{'═'*50}")
    logger.info(f"INGEST COMPLETE in {elapsed:.0f}s")
    logger.info(f"{'═'*50}")
    for source, n in results.items():
        icon = "✅" if n > 0 else "⚠️ "
        logger.info(f"  {icon}  {source:<12} +{n} rows")

    total_opps  = con.execute("SELECT COUNT(*) FROM unified_opportunities").fetchone()[0]
    total_fails = con.execute("SELECT COUNT(*) FROM failures_unified").fetchone()[0]
    total_enr   = con.execute("SELECT COUNT(*) FROM enriched_details").fetchone()[0]
    logger.info(f"\n  DB totals:")
    logger.info(f"    unified_opportunities : {total_opps:,}")
    logger.info(f"    failures_unified      : {total_fails:,}")
    logger.info(f"    enriched_details      : {total_enr:,}")

    if args.build_index:
        logger.info("\nBuilding search indexes...")
        scripts_dir = Path(__file__).resolve().parent
        import subprocess
        for script in ["build_tfidf.py", "build_embeddings.py"]:
            script_path = scripts_dir / script
            if script_path.exists():
                logger.info(f"  Running {script}...")
                subprocess.run([sys.executable, str(script_path)], check=False)
            else:
                logger.warning(f"  {script} not found.")


if __name__ == "__main__":
    main()

"""                              
  python scripts/ingest.py --source grants       
  python scripts/ingest.py --build-index          
"""
