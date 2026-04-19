import duckdb
from loguru import logger
from typing import Optional

from storage.db import get_db


UNIFIED_OPPORTUNITIES = """
CREATE TABLE IF NOT EXISTS unified_opportunities (
    opp_id          VARCHAR PRIMARY KEY,
    source          VARCHAR NOT NULL,        -- 'grants' | 'sam' | 'patents' | 'research'
    title           VARCHAR NOT NULL,
    description     TEXT,
    sector          VARCHAR,
    naics_code      VARCHAR,
    posted_date     DATE,
    close_date      DATE,
    funding_min     DOUBLE,
    funding_max     DOUBLE,
    agency          VARCHAR,
    geography       VARCHAR,
    eligibility     VARCHAR,
    tags            VARCHAR[],
    raw_json        JSON,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

FAILURES_UNIFIED = """
CREATE TABLE IF NOT EXISTS failures_unified (
    failure_id          VARCHAR PRIMARY KEY,
    company_name        VARCHAR NOT NULL,
    sector              VARCHAR,
    naics_code          VARCHAR,
    year_founded        INTEGER,
    year_failed         INTEGER,
    funding_raised_usd  DOUBLE,
    failure_reasons     VARCHAR[],
    stage_at_failure    VARCHAR,
    key_lesson          TEXT,
    founder_names       VARCHAR,
    source_url          VARCHAR,
    raw_json            JSON,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

ENRICHED_DETAILS = """
CREATE TABLE IF NOT EXISTS enriched_details (
    record_id           VARCHAR  NOT NULL,
    record_type         VARCHAR  NOT NULL,   -- 'opportunity' | 'failure'
    source              VARCHAR  NOT NULL,   -- 'grants' | 'sam' | 'research' | 'patents' | 'failory' | 'cbinsights'
    full_text           TEXT,               -- complete text blob (capped 50K chars)
    summary             TEXT,               -- 4-sentence extractive summary
    key_fields          JSON,               -- all extra structured fields
    tfidf_vector        JSON,               -- {term: weight} sparse dict (top 50 terms)
    embedding           FLOAT[768],         -- sentence-transformers all-mpnet-base-v2
    enriched_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    enrichment_status   VARCHAR DEFAULT 'done',  -- 'done' | 'failed' | 'pending'
    error_msg           TEXT,
    PRIMARY KEY (record_id, source)
);
"""

WATCHLIST = """
CREATE TABLE IF NOT EXISTS watchlist (
    id          INTEGER PRIMARY KEY,
    user_label  VARCHAR NOT NULL,
    keyword     VARCHAR,
    sectors     VARCHAR[],
    min_funding DOUBLE DEFAULT 0,
    sources     VARCHAR[],
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_alerted TIMESTAMP
);
"""

PIPELINE_LOG = """
CREATE TABLE IF NOT EXISTS pipeline_log (
    run_id      VARCHAR PRIMARY KEY,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP,
    source      VARCHAR,
    rows_added  INTEGER,
    status      VARCHAR,    -- 'ok' | 'error'
    error_msg   TEXT
);
"""



SECTOR_SUMMARY_VIEW = """
CREATE OR REPLACE VIEW sector_summary AS
SELECT
    o.sector,
    COUNT(*)                                              AS total_opps,
    COUNT(*) FILTER (WHERE o.source = 'sam')             AS contracts,
    COUNT(*) FILTER (WHERE o.source = 'grants')          AS grants,
    COUNT(*) FILTER (WHERE o.source = 'patents')         AS patents,
    COUNT(*) FILTER (WHERE o.source = 'research')        AS research,
    AVG(o.funding_max) FILTER (WHERE o.funding_max > 0)  AS avg_funding,
    MAX(o.funding_max) FILTER (WHERE o.funding_max > 0)  AS max_funding,
    MAX(o.posted_date)                                   AS last_updated,
    COUNT(f.failure_id)                                  AS known_failures
FROM unified_opportunities o
LEFT JOIN failures_unified f
    ON LOWER(f.sector) = LOWER(o.sector)
GROUP BY o.sector
ORDER BY total_opps DESC;
"""

OPEN_OPPORTUNITIES_VIEW = """
CREATE OR REPLACE VIEW open_opportunities AS
SELECT * FROM unified_opportunities
WHERE
    (
        source IN ('sam', 'grants')
        AND (close_date IS NULL OR close_date >= CURRENT_DATE)
        AND posted_date >= CURRENT_DATE - INTERVAL '180 DAYS'
    )
    OR source IN ('patents', 'research');
"""


INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_opp_source   ON unified_opportunities(source);",
    "CREATE INDEX IF NOT EXISTS idx_opp_sector   ON unified_opportunities(sector);",
    "CREATE INDEX IF NOT EXISTS idx_opp_date     ON unified_opportunities(posted_date);",
    "CREATE INDEX IF NOT EXISTS idx_opp_funding  ON unified_opportunities(funding_max);",
    "CREATE INDEX IF NOT EXISTS idx_fail_sector  ON failures_unified(sector);",
    "CREATE INDEX IF NOT EXISTS idx_fail_year    ON failures_unified(year_failed);",
    "CREATE INDEX IF NOT EXISTS idx_enr_id       ON enriched_details(record_id);",
    "CREATE INDEX IF NOT EXISTS idx_enr_source   ON enriched_details(source);",
    "CREATE INDEX IF NOT EXISTS idx_enr_status   ON enriched_details(enrichment_status);",
]



def create_schema(con: Optional[duckdb.DuckDBPyConnection] = None) -> None:
    if con is None:
        con = get_db()

    tables = {
        "unified_opportunities": UNIFIED_OPPORTUNITIES,
        "failures_unified":      FAILURES_UNIFIED,
        "enriched_details":      ENRICHED_DETAILS,
        "watchlist":             WATCHLIST,
        "pipeline_log":          PIPELINE_LOG,
    }
    for name, ddl in tables.items():
        con.execute(ddl)
        logger.debug(f"Table ready: {name}")

    con.execute(SECTOR_SUMMARY_VIEW)
    con.execute(OPEN_OPPORTUNITIES_VIEW)

    for ddl in INDEXES:
        con.execute(ddl)

    logger.success("Schema ready — all tables, views, and indexes confirmed.")


def drop_all(con: Optional[duckdb.DuckDBPyConnection] = None) -> None:
    if con is None:
        con = get_db()
    for tbl in ["enriched_details", "embeddings", "watchlist", "pipeline_log",
                "failures_unified", "unified_opportunities"]:
        con.execute(f"DROP TABLE IF EXISTS {tbl};")
    for view in ["sector_summary", "open_opportunities"]:
        con.execute(f"DROP VIEW IF EXISTS {view};")
    logger.warning("All tables and views dropped.")


if __name__ == "__main__":
    create_schema()
    con = get_db()
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY 1"
    ).fetchall()
    logger.info("Tables created:")
    for (t,) in tables:
        logger.info(f"  • {t}")