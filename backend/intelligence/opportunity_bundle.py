import math
import pandas as pd
from loguru import logger

from intelligence.risk_scorer      import score_sector_risk
from intelligence.market_validator  import validate_market
from intelligence.competitor_radar  import get_competitor_radar
from intelligence.white_space       import get_sector_white_space


def _safe_fillna(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype.kind in ("i", "u"):
            df[col] = df[col].fillna(0)
        elif df[col].dtype.kind == "f":
            df[col] = df[col].fillna(0.0)
        else:
            df[col] = df[col].fillna("")
    return df


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


def build_opportunity_bundle(sector: str, con) -> dict:
    
    validation  = validate_market(sector, con)
    risk        = score_sector_risk(sector, con)
    competitors = get_competitor_radar(sector, con)
    white_space = get_sector_white_space(sector, con)

    contracts = _safe_fillna(con.execute("""
        SELECT
            opp_id, title, agency, funding_max,
            close_date::VARCHAR AS close_date,
            posted_date::VARCHAR AS posted_date,
            array_to_string(tags, ',') AS tags
        FROM unified_opportunities
        WHERE source = 'sam' AND sector ILIKE ?
        AND (close_date IS NULL OR close_date >= CURRENT_DATE)
        ORDER BY funding_max DESC NULLS LAST
        LIMIT 5
    """, [f"%{sector}%"]).fetchdf()).to_dict(orient="records")

    grants = _safe_fillna(con.execute("""
        SELECT
            opp_id, title, agency, funding_max,
            close_date::VARCHAR AS close_date,
            posted_date::VARCHAR AS posted_date,
            eligibility
        FROM unified_opportunities
        WHERE source = 'grants' AND sector ILIKE ?
        AND (close_date IS NULL OR close_date >= CURRENT_DATE)
        ORDER BY close_date ASC NULLS LAST
        LIMIT 5
    """, [f"%{sector}%"]).fetchdf()).to_dict(orient="records")

    patents = _safe_fillna(con.execute("""
        SELECT
            opp_id, title, agency,
            posted_date::VARCHAR AS posted_date,
            array_to_string(tags, ',') AS tags
        FROM unified_opportunities
        WHERE source = 'patents' AND sector ILIKE ?
        ORDER BY posted_date DESC NULLS LAST
        LIMIT 5
    """, [f"%{sector}%"]).fetchdf()).to_dict(orient="records")

    research = _safe_fillna(con.execute("""
        SELECT
            opp_id, title, agency,
            posted_date::VARCHAR AS posted_date,
            array_to_string(tags, ',') AS tags
        FROM unified_opportunities
        WHERE source = 'research' AND sector ILIKE ?
        ORDER BY posted_date DESC NULLS LAST
        LIMIT 5
    """, [f"%{sector}%"]).fetchdf()).to_dict(orient="records")


    failures = _safe_fillna(con.execute("""
        SELECT
            failure_id, company_name, sector,
            year_failed, funding_raised_usd,
            failure_reasons, key_lesson, founder_names
        FROM failures_unified
        WHERE sector ILIKE ?
        ORDER BY funding_raised_usd DESC NULLS LAST
        LIMIT 5
    """, [f"%{sector}%"]).fetchdf()).to_dict(orient="records")

    return {
        "sector":       sector,
        "validation":   validation,
        "risk":         risk,
        "white_space":  white_space,
        "competitors":  competitors,
        "contracts":    [_clean(r) for r in contracts],
        "grants":       [_clean(r) for r in grants],
        "patents":      [_clean(r) for r in patents],
        "research":     [_clean(r) for r in research],
        "failures":     [_clean(r) for r in failures],
    }