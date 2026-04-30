"""
Database layer
==============
On startup:
  1. Downloads parquet files from Cloudflare R2 into /tmp/failsight/
  2. Loads them into an in-memory DuckDB instance
  3. Downloads TF-IDF + embedding index files from R2

Falls back to local data.duckdb in dev if R2 is not configured.
"""

import os
from pathlib import Path
from loguru import logger
import duckdb

ROOT          = Path(__file__).resolve().parent.parent
DATA_DIR      = ROOT / "data"
INDEX_DIR     = DATA_DIR / "search_index"
PARQUET_DIR   = DATA_DIR / "parquet"
LEGACY_DB_PATH = ROOT / "data.duckdb"

TMP_BASE    = Path(os.environ.get("ASSET_CACHE_DIR", "/tmp/failsight"))
TMP_PARQUET = TMP_BASE / "parquet"
TMP_INDEX   = TMP_BASE / "index"

_DB: duckdb.DuckDBPyConnection | None = None
_R2_ENABLED: bool = bool(
    os.environ.get("R2_ACCOUNT_ID") and
    os.environ.get("R2_ACCESS_KEY_ID") and
    os.environ.get("R2_SECRET_ACCESS_KEY")
)


def _install_extensions(con: duckdb.DuckDBPyConnection) -> None:
    for ext in ("json", "parquet", "fts"):
        try:
            con.execute(f"INSTALL {ext}; LOAD {ext};")
        except Exception as e:
            logger.debug(f"Extension '{ext}': {e}")


def export_parquet(con: duckdb.DuckDBPyConnection, out_dir: Path | None = None) -> Path:
    out = out_dir or PARQUET_DIR
    out.mkdir(parents=True, exist_ok=True)
    tables = {
        "unified_opportunities": out / "unified_opportunities.parquet",
        "failures_unified":      out / "failures_unified.parquet",
        "enriched_details":      out / "enriched_details.parquet",
    }
    for table, path in tables.items():
        try:
            con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            size_mb = path.stat().st_size / 1024 / 1024
            logger.info(f"Exported {table} → {path} ({size_mb:.1f} MB)")
        except Exception as e:
            logger.warning(f"Could not export {table}: {e}")
    return out


def _load_from_parquet(parquet_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    _install_extensions(con)
    from storage.schema import create_schema
    create_schema(con)

    # All three tables loaded the same way — no special view treatment
    tables = {
        "unified_opportunities": parquet_dir / "unified_opportunities.parquet",
        "failures_unified":      parquet_dir / "failures_unified.parquet",
        "enriched_details":      parquet_dir / "enriched_details.parquet",
    }
    for table, path in tables.items():
        if path.exists():
            try:
                con.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{path}')")
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                logger.info(f"Loaded {table}: {count:,} rows")
            except Exception as e:
                logger.error(f"Failed to load {table}: {e}")
        else:
            logger.warning(f"Parquet not found — {table} will be empty: {path}")

    return con


def bootstrap_from_r2() -> duckdb.DuckDBPyConnection:
    from storage.r2_store import download_data_assets, R2CapExceeded, R2Unavailable

    TMP_PARQUET.mkdir(parents=True, exist_ok=True)
    TMP_INDEX.mkdir(parents=True, exist_ok=True)

    logger.info("Bootstrapping database from Cloudflare R2…")
    try:
        result = download_data_assets(TMP_PARQUET, TMP_INDEX, skip_embeddings=False)
        logger.info(f"R2 download: {len(result['downloaded'])} files, {len(result['missing'])} missing.")
    except R2CapExceeded as e:
        logger.error(f"R2 cap exceeded during startup: {e}")
        raise RuntimeError(
            "R2 read cap exceeded. The application cannot load data. "
            "Caps reset on the 1st of each month."
        ) from e
    except R2Unavailable as e:
        logger.warning(f"R2 unavailable: {e}. Falling back to local DB.")
        return _fallback_db()

    con = _load_from_parquet(TMP_PARQUET)
    logger.success("In-memory DuckDB populated from R2 parquet files.")
    return con


def _fallback_db() -> duckdb.DuckDBPyConnection:
    LEGACY_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Using local DB: {LEGACY_DB_PATH}")
    con = duckdb.connect(str(LEGACY_DB_PATH), read_only=False)
    _install_extensions(con)
    return con


def init_db() -> duckdb.DuckDBPyConnection:
    global _DB
    if _R2_ENABLED:
        _DB = bootstrap_from_r2()
    else:
        logger.info("R2 not configured — using local/fallback DB.")
        _DB = _fallback_db()
    return _DB


def get_db() -> duckdb.DuckDBPyConnection:
    global _DB
    if _DB is not None:
        try:
            return _DB.cursor()
        except Exception:
            pass
    return _fallback_db()


def reload_db_from_parquet(parquet_dir: Path | None = None) -> duckdb.DuckDBPyConnection:
    global _DB
    src = parquet_dir or TMP_PARQUET
    logger.info(f"Reloading in-memory DB from {src}…")
    _DB = _load_from_parquet(src)
    logger.success("In-memory DB reloaded.")
    return _DB