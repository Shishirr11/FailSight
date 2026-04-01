import duckdb
from pathlib import Path
from loguru import logger
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data.duckdb"


def get_db(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH), read_only=read_only)
    _install_extensions(con)
    return con



def _install_extensions(con: duckdb.DuckDBPyConnection) -> None:
    for ext in ("json", "parquet", "fts", "vss"):
        try:
            con.execute(f"INSTALL {ext}; LOAD {ext};")
        except Exception as e:
            logger.debug(f"Extension '{ext}': {e}")


if __name__ == "__main__":
    con = get_db()
    version = con.execute("SELECT version()").fetchone()[0]
    logger.success(f"DuckDB connected — version {version}")
    logger.success(f"Database path: {DB_PATH}")