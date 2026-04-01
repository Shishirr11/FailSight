#python scripts/init_db.py --drop 

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))


from storage.db import get_db
from storage.schema import create_schema, drop_all
from loguru import logger


def main():
    drop = "--drop" in sys.argv

    con = get_db()

    if drop:
        logger.warning("--drop flag detected — wiping all tables and views.")
        drop_all(con)

    create_schema(con)
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()

    logger.success("Database initialized. Tables:")
    for (t,) in tables:
        logger.info(f"  • {t}")


if __name__ == "__main__":
    main()