import sys
import argparse
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "backend"))

from loguru import logger

SCRIPTS_DIR = Path(__file__).resolve().parent
SEARCH_DIR  = SCRIPTS_DIR.parent / "search"
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(SEARCH_DIR))


def run_step(name: str, fn, **kwargs):
    """Run one enrichment step with timing and error isolation."""
    logger.info(f"\n{'='*55}")
    logger.info(f"STEP: {name}")
    logger.info(f"{'='*55}")
    start = time.time()
    try:
        fn(**kwargs)
        elapsed = time.time() - start
        logger.success(f"{name} completed in {elapsed:.1f}s")
        return True
    except Exception as e:
        logger.error(f"{name} FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(description="Run the full enrichment pipeline")
    parser.add_argument("--only",         type=str, default=None,
                        choices=["grants", "research", "failory", "tfidf", "embeddings"],
                        help="Run only one specific step")
    parser.add_argument("--skip-index",   action="store_true",
                        help="Skip building TF-IDF and embeddings indexes")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Retry previously failed enrichments")
    parser.add_argument("--limit",        type=int, default=None,
                        help="Limit records per enricher (for testing)")
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"Enrichment pipeline starting — {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    shared = {"limit": args.limit, "retry_failed": args.retry_failed}

    from scripts.migrate_schema import migrate
    run_step("Schema migration", migrate)

    if args.only:
        if args.only == "grants":
            from enrich_grants import run as run_grants
            run_step("Grants enrichment", run_grants, **shared)
        elif args.only == "research":
            from enrich_research import run as run_research
            run_step("Research enrichment", run_research, **shared)
        elif args.only == "failory":
            from enrich_failory import run as run_failory
            run_step("Failory enrichment", run_failory, **shared)
        elif args.only == "tfidf":
            from build_tfidf import build as build_tfidf
            run_step("TF-IDF index build", build_tfidf)
        elif args.only == "embeddings":
            from build_embeddings import build as build_embeddings
            run_step("Embedding generation", build_embeddings)
        return

    results = {}

    from enrich_grants   import run as run_grants
    from enrich_research import run as run_research
    from enrich_failory  import run as run_failory

    results["grants"]   = run_step("Grants enrichment",   run_grants,   **shared)
    results["research"] = run_step("Research enrichment", run_research, **shared)
    results["failory"]  = run_step("Failory enrichment",  run_failory,  **shared)

    if not args.skip_index:
        from build_tfidf      import build as build_tfidf
        from build_embeddings import build as build_embeddings
        results["tfidf"]      = run_step("TF-IDF index build",    build_tfidf)
        results["embeddings"] = run_step("Embedding generation",   build_embeddings)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"\n{'='*55}")
    logger.info(f"ENRICHMENT PIPELINE COMPLETE in {elapsed:.0f}s")
    logger.info(f"{'='*55}")
    for step, ok in results.items():
        icon = "\/" if ok else "X"
        logger.info(f"  {icon} {step}")

    from storage.db import get_db
    con = get_db()
    counts = con.execute("""
        SELECT source, enrichment_status, COUNT(*) as n
        FROM enriched_details
        GROUP BY source, enrichment_status
        ORDER BY source, enrichment_status
    """).fetchall()

    logger.info("\nEnriched records by source:")
    for source, status, n in counts:
        icon = "\/" if status == "done" else "X"
        logger.info(f"  {icon} {source:<15} {status:<10} {n:>6}")


if __name__ == "__main__":
    main()