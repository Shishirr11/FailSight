import sys
import shutil
import argparse
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from loguru import logger
from storage.db import get_db, DB_PATH
from storage.schema import create_schema, drop_all

RAW_DIR       = ROOT / "backend" / "data" / "raw"
PROCESSED_DIR = ROOT / "backend" / "data" / "processed"
CBINSIGHTS_DIR = RAW_DIR / "failures" / "cbinsights"   

def _confirm(msg: str) -> bool:
    ans = input(f"\n  {msg} [y/N]: ").strip().lower()
    return ans == "y"

def soft_reset(con):

    logger.warning("Soft reset: truncating all tables...")
    tables = [
        "enriched_details",
        "pipeline_log",
        "watchlist",
        "failures_unified",
        "unified_opportunities",
    ]
    for t in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        con.execute(f"DELETE FROM {t}")
        logger.info(f"  {t}: {count} rows deleted")

    create_schema(con)
    logger.success("Soft reset complete — tables empty, DB file kept.")

def full_reset():


    logger.warning("Full reset: deleting DB file, raw JSON, and processed Parquet...")

    if RAW_DIR.exists():
        for source_dir in sorted(RAW_DIR.iterdir()):
            if not source_dir.is_dir():
                continue
            if source_dir == CBINSIGHTS_DIR or source_dir.name == "cbinsights":
                logger.info(f"  Skipping {source_dir} — CB Insights CSVs preserved")
                continue

            jsons = list(source_dir.glob("*.json"))
            for f in jsons:
                f.unlink()
            if jsons:
                logger.info(f"  Deleted {len(jsons)} raw JSON file(s) from {source_dir.name}/")

    if PROCESSED_DIR.exists():
        shutil.rmtree(PROCESSED_DIR)
        logger.info(f"  Deleted processed/ directory ({PROCESSED_DIR})")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"  Recreated empty processed/ directory")

    if DB_PATH.exists():
        size_mb = DB_PATH.stat().st_size / (1024 * 1024)
        DB_PATH.unlink()
        logger.info(f"  Deleted {DB_PATH.name} ({size_mb:.1f} MB)")

    con = get_db()
    create_schema(con)
    logger.success("Full reset complete — fresh empty database ready.")
    return con

def main():
    parser = argparse.ArgumentParser(
        description="Reset Findout database and optionally re-ingest",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--soft", action="store_true"
                    )
    mode.add_argument("--full", action="store_true"
                      )
    parser.add_argument("--reingest", action="store_true"
                        )
    parser.add_argument("--no-detail", action="store_true"
                       )
    parser.add_argument("--skip-failures", action="store_true")
    parser.add_argument("--source", choices=["grants","sam","research","patents","failures"],
                        default=None)
    parser.add_argument("--yes", action="store_true",
    )
    args = parser.parse_args()

    print()
    if args.full:
        print("       FULL RESET — this will permanently delete:")
        print("      • The entire DuckDB database")
        print("      • All raw JSON files (CB Insights CSVs are safe)")
        print("      • All processed Parquet files")
    else:
        print("       SOFT RESET — this will delete:")
        print("      • All rows in unified_opportunities, failures_unified,")
        print("        enriched_details, watchlist, pipeline_log")
        print("      • Raw files and Parquet stay on disk")

    if args.reingest:
        src = args.source or "all sources"
        print(f"      • Then immediately re-ingest: {src}")

    if not args.yes and not _confirm("Are you sure you want to continue?"):
        print("  Aborted.")
        sys.exit(0)

    if args.full:
        con = full_reset()
    else:
        con = get_db()
        soft_reset(con)

    opp_count  = con.execute("SELECT COUNT(*) FROM unified_opportunities").fetchone()[0]
    fail_count = con.execute("SELECT COUNT(*) FROM failures_unified").fetchone()[0]
    enr_count  = con.execute("SELECT COUNT(*) FROM enriched_details").fetchone()[0]
    logger.info(f"DB state after reset: opportunities={opp_count} | "
                f"failures={fail_count} | enriched={enr_count}")

    if opp_count > 0 or fail_count > 0:
        logger.error("Reset did not fully clear tables — check for errors above.")
        sys.exit(1)

    if args.reingest:
        logger.info("\nStarting fresh ingest...")
        ingest_script = ROOT / "scripts" / "ingest.py"
        cmd = [sys.executable, str(ingest_script)]

        if args.source:
            cmd += ["--source", args.source]
        if args.no_detail:
            cmd.append("--no-detail")
        if args.skip_failures:
            cmd.append("--skip-failures")

        logger.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd)
        sys.exit(result.returncode)
    else:
        print()
        print("  Reset done. Next steps:")
        print()
        if args.soft:
            print("  # Reload from existing raw files (fastest):")
            print("  python scripts/ingest.py --disk")
            print()
        print("  # Full fresh ingest from live APIs (recommended):")
        print("  python scripts/ingest.py")
if __name__ == "__main__":
    main()

"""
    python scripts/reset_db.py --soft          
    python scripts/reset_db.py --full          
    python scripts/reset_db.py --full --reingest   
"""
