"""
Still not sure if ill use this file so this is garbage only..
"""

import math
from fastapi import APIRouter, Query
from loguru import logger

from storage.db import get_db
from intelligence.risk_scorer       import score_sector_risk
from intelligence.market_validator  import validate_market
from intelligence.competitor_radar  import get_competitor_radar
from intelligence.white_space       import detect_white_spaces, get_sector_white_space
from intelligence.opportunity_bundle import build_opportunity_bundle

router = APIRouter()


def _clean(record: dict) -> dict:
    out = {}
    for k, v in record.items():
        if hasattr(v, "tolist"):
            v = v.tolist()
        elif hasattr(v, "item") and hasattr(v, "ndim") and v.ndim == 0:
            v = v.item()
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            v = None
        out[k] = v
    return out

def _clean_rows(rows): return [_clean(r) for r in rows]


@router.get("")
def list_sectors():
    con = get_db()

    sectors = con.execute("""
        SELECT
            o.sector,
            COUNT(*)                                                    AS total_opps,
            COUNT(*) FILTER (WHERE o.source = 'sam')                   AS contracts,
            COUNT(*) FILTER (WHERE o.source = 'grants')                AS grants,
            COUNT(*) FILTER (WHERE o.source = 'patents')               AS patents,
            COUNT(*) FILTER (WHERE o.source = 'research')              AS research,
            COALESCE(AVG(o.funding_max)
                FILTER (WHERE o.funding_max > 0), 0)                   AS avg_funding,
            MAX(o.posted_date)::VARCHAR                                 AS last_updated
        FROM unified_opportunities o
        WHERE o.sector IS NOT NULL AND o.sector != 'Other'
        GROUP BY o.sector
        ORDER BY total_opps DESC
    """).fetchdf().fillna(0)

    failures = con.execute("""
        SELECT sector, COUNT(*) AS failure_count
        FROM failures_unified
        WHERE sector IS NOT NULL
        GROUP BY sector
    """).fetchdf()
    merged = sectors.merge(failures, on="sector", how="left").fillna(0)

    return {
        "sectors": _clean_rows(merged.to_dict(orient="records"))
    }

@router.get("/heatmap")
def sector_heatmap(days: int = Query(180, description="Lookback window in days")):
    con = get_db()

    rows = con.execute("""
        SELECT
            sector,
            source,
            COUNT(*)                                    AS count,
            COALESCE(AVG(funding_max)
                FILTER (WHERE funding_max > 0), 0)      AS avg_funding
        FROM unified_opportunities
        WHERE sector IS NOT NULL
          AND sector != 'Other'
          AND posted_date >= CURRENT_DATE - INTERVAL (? || ' DAYS')
        GROUP BY sector, source
        ORDER BY sector, source
    """, [days]).fetchdf().fillna(0)

    return {"heatmap": _clean_rows(rows.to_dict(orient="records"))}

@router.get("/white-space")
def white_space_all(min_innovation: int = Query(5)):
    con     = get_db()
    results = detect_white_spaces(con, min_innovation=min_innovation)
    return {"white_space": _clean_rows(results)}


@router.get("/{sector_name}")
def get_sector_bundle(sector_name: str):
    con    = get_db()
    bundle = build_opportunity_bundle(sector_name, con)
    return bundle

@router.get("/{sector_name}/validation")
def get_sector_validation(sector_name: str):
    con = get_db()
    return validate_market(sector_name, con)


@router.get("/{sector_name}/risk")
def get_sector_risk(sector_name: str):
    con = get_db()
    return score_sector_risk(sector_name, con)


@router.get("/{sector_name}/competitors")
def get_sector_competitors(sector_name: str):
    con = get_db()
    return get_competitor_radar(sector_name, con)


@router.get("/{sector_name}/white-space")
def get_sector_whitespace(sector_name: str):
    con = get_db()
    return get_sector_white_space(sector_name, con)