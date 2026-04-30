"""
One-time migration: exports data.duckdb → parquet → uploads to R2

Usage:
    export R2_ACCOUNT_ID=your_account_id
    export R2_ACCESS_KEY_ID=your_key_id
    export R2_SECRET_ACCESS_KEY=your_secret
    export R2_BUCKET_NAME=failsight-data

    python scripts/migrate_to_r2.py

    # Dry run (export only, no upload):
    python scripts/migrate_to_r2.py --dry-run
"""

import sys
import argparse
from pathlib import Path

HERE    = Path(__file__).resolve().parent
BACKEND = HERE.parent
sys.path.insert(0, str(BACKEND))
sys.path.insert(0, str(HERE))

from loguru import logger

from pathlib import Path as _Path
for _env in [_Path(__file__).parent.parent.parent / ".env", 
             _Path(__file__).parent.parent / ".env"]:
    if _env.exists():
        import os
        with open(_env) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _k, _, _v = _line.partition("=")
                    os.environ.setdefault(_k.strip(), _v.strip())
        break


def migrate(dry_run: bool = False, skip_embeddings: bool = False):

    db_path = BACKEND / "data.duckdb"
    if not db_path.exists():
        db_path = HERE.parent / "data.duckdb"
    if not db_path.exists():
        logger.error("data.duckdb not found.")
        sys.exit(1)

    logger.info(f"Found: {db_path} ({db_path.stat().st_size / 1024**3:.2f} GB)")

    import duckdb
    out_dir = HERE.parent / "_migration_parquet"
    out_dir.mkdir(exist_ok=True)

    con = duckdb.connect(str(db_path), read_only=True)
    for ext in ("json", "parquet"):
        try:
            con.execute(f"INSTALL {ext}; LOAD {ext};")
        except Exception:
            pass

    tables = ["unified_opportunities", "failures_unified", "enriched_details"]
    exported = {}

    for table in tables:
        out_path = out_dir / f"{table}.parquet"
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            con.execute(f"COPY {table} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            size_mb = out_path.stat().st_size / 1024 / 1024
            logger.success(f"{table}: {count:,} rows → {size_mb:.1f} MB")
            exported[table] = out_path
        except Exception as e:
            logger.error(f"Failed to export {table}: {e}")

    con.close()

    index_dir = BACKEND / "data" / "search_index"
    index_files = {
        "tfidf_matrix.npy":      index_dir / "tfidf_matrix.npy",
        "tfidf_vectorizer.pkl":  index_dir / "tfidf_vectorizer.pkl",
        "tfidf_record_ids.json": index_dir / "tfidf_record_ids.json",
    }
    if not skip_embeddings:
        index_files["embeddings_matrix.npy"]     = index_dir / "embeddings_matrix.npy"
        index_files["embedding_record_ids.json"] = index_dir / "embedding_record_ids.json"

    found_indexes = {k: v for k, v in index_files.items() if v.exists()}

    if dry_run:
        logger.info(f"DRY RUN complete. Parquet files in: {out_dir}")
        return

    import os
    missing_env = [v for v in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY") if not os.environ.get(v)]
    if missing_env:
        logger.error(f"Missing env vars: {missing_env}")
        sys.exit(1)

    from storage.r2_store import upload_file, R2CapExceeded, R2Unavailable, KEYS

    upload_map = {
        "unified_opportunities": KEYS["opportunities_parquet"],
        "failures_unified":      KEYS["failures_parquet"],
        "enriched_details":      KEYS["enriched_parquet"],
    }
    for table, local_path in exported.items():
        r2_key = upload_map.get(table)
        if not r2_key:
            continue
        try:
            upload_file(local_path, r2_key)
        except (R2CapExceeded, R2Unavailable) as e:
            logger.error(f"Upload failed: {e}")
            sys.exit(1)

    index_key_map = {
        "tfidf_matrix.npy":            KEYS["tfidf_matrix"],
        "tfidf_vectorizer.pkl":        KEYS["tfidf_vectorizer"],
        "tfidf_record_ids.json":       KEYS["tfidf_ids"],
        "embeddings_matrix.npy":       KEYS["embed_matrix"],
        "embedding_record_ids.json":   KEYS["embed_ids"],
    }
    for name, local_path in found_indexes.items():
        r2_key = index_key_map.get(name)
        if not r2_key:
            continue
        try:
            upload_file(local_path, r2_key)
        except R2CapExceeded as e:
            logger.error(f"Cap exceeded: {e}")
            break
        except R2Unavailable as e:
            logger.warning(f"Skipped {name}: {e}")

    logger.success("\n Migration complete!")
    logger.success("Add these env vars to Railway:")
    logger.success(f"  R2_ACCOUNT_ID        = {os.environ.get('R2_ACCOUNT_ID')}")
    logger.success(f"  R2_ACCESS_KEY_ID     = {os.environ.get('R2_ACCESS_KEY_ID')}")
    logger.success(f"  R2_SECRET_ACCESS_KEY = ***")
    logger.success(f"  R2_BUCKET_NAME       = {os.environ.get('R2_BUCKET_NAME', 'failsight-data')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",         action="store_true")
    parser.add_argument("--skip-embeddings", action="store_true")
    args = parser.parse_args()
    migrate(dry_run=args.dry_run, skip_embeddings=args.skip_embeddings)