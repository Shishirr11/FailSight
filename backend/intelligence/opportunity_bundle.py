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


def _sector_keywords(sector: str) -> list[str]:
    """
    Extract meaningful keywords from a sector label for fallback queries.
    e.g. 'AI & Machine Learning' -> ['AI', 'Machine Learning', 'artificial intelligence']
    """
    # Predefined keyword expansions for known sectors
    EXPANSIONS = {
        "AI & Machine Learning":  ["artificial intelligence", "machine learning", "deep learning", "neural", "AI"],
        "Cybersecurity":          ["cybersecurity", "cyber security", "security", "threat", "encryption"],
        "Clean Energy":           ["renewable energy", "solar", "wind energy", "energy storage", "battery"],
        "Climate Technology":     ["climate", "carbon", "emissions", "sustainability", "greenhouse"],
        "Biotechnology":          ["biotech", "genomics", "crispr", "protein", "gene therapy"],
        "Health Technology":      ["health", "medical", "telehealth", "clinical", "diagnostic"],
        "Quantum Computing":      ["quantum", "qubit", "quantum computing"],
        "Advanced Manufacturing": ["manufacturing", "additive", "3d printing", "robotics", "automation"],
        "Aerospace & Defense":    ["aerospace", "defense", "drone", "satellite", "propulsion"],
        "Agriculture Technology": ["agriculture", "agtech", "precision agriculture", "crop", "food safety"],
        "Advanced Computing":     ["semiconductor", "chip", "microelectronics", "computing", "photonics"],
        "Fintech":                ["fintech", "financial technology", "payment", "banking", "finance"],
        "Transportation":         ["transportation", "mobility", "electric vehicle", "autonomous"],
        "Infrastructure":         ["infrastructure", "broadband", "grid", "bridge", "water"],
        "Education":              ["education", "edtech", "learning", "workforce", "training"],
        "Small Business":         ["small business", "startup", "entrepreneurship", "innovation"],
        "Community Development":  ["community", "affordable housing", "resilience", "disaster"],
        "Real Estate":            ["real estate", "property", "proptech", "housing", "commercial"],
    }
    if sector in EXPANSIONS:
        return EXPANSIONS[sector]
    # Generic fallback: use words from the sector name (3+ chars)
    words = [w for w in sector.replace("&", "").replace("-", " ").split() if len(w) >= 3]
    return words[:4]


def _query_with_fallback(con, sql_exact: str, sql_fallback: str,
                         params_exact: list, params_fallback: list,
                         min_rows: int = 2) -> pd.DataFrame:
    """
    Try exact sector ILIKE query first; if fewer than min_rows results,
    fall back to keyword-based title/description search.
    """
    df = _safe_fillna(con.execute(sql_exact, params_exact).fetchdf())
    if len(df) >= min_rows:
        return df
    logger.debug(f"Sector exact match returned {len(df)} rows — trying keyword fallback")
    return _safe_fillna(con.execute(sql_fallback, params_fallback).fetchdf())


def _kw_cond(keywords: list[str], field: str = "title") -> tuple[str, list]:
    """Build an OR-based ILIKE condition for a list of keywords."""
    if not keywords:
        return "1=0", []
    parts = " OR ".join(f"{field} ILIKE ?" for _ in keywords)
    params = [f"%{kw}%" for kw in keywords]
    return f"({parts})", params


def build_opportunity_bundle(sector: str, con) -> dict:
    kws = _sector_keywords(sector)
    sector_p = f"%{sector}%"

    # ── Title/desc keyword condition ──────────────────────────────────────
    kw_title_cond, kw_title_p = _kw_cond(kws, "title")
    kw_desc_cond,  kw_desc_p  = _kw_cond(kws, "description")
    kw_any_cond = f"({kw_title_cond} OR {kw_desc_cond})"
    kw_any_p    = kw_title_p + kw_desc_p

    # ── Intelligence modules (these already have their own fallbacks) ─────
    validation  = validate_market(sector, con)
    risk        = score_sector_risk(sector, con)
    competitors = get_competitor_radar(sector, con)
    white_space = get_sector_white_space(sector, con)

    # ── Contracts ─────────────────────────────────────────────────────────
    contracts = _query_with_fallback(
        con,
        sql_exact="""
            SELECT opp_id, title, agency, funding_max,
                   close_date::VARCHAR AS close_date,
                   posted_date::VARCHAR AS posted_date,
                   array_to_string(tags, ',') AS tags
            FROM unified_opportunities
            WHERE source = 'sam' AND sector ILIKE ?
              AND (close_date IS NULL OR close_date >= CURRENT_DATE)
            ORDER BY funding_max DESC NULLS LAST
            LIMIT 5
        """,
        sql_fallback=f"""
            SELECT opp_id, title, agency, funding_max,
                   close_date::VARCHAR AS close_date,
                   posted_date::VARCHAR AS posted_date,
                   array_to_string(tags, ',') AS tags
            FROM unified_opportunities
            WHERE source = 'sam'
              AND (close_date IS NULL OR close_date >= CURRENT_DATE)
              AND {kw_any_cond}
            ORDER BY funding_max DESC NULLS LAST
            LIMIT 5
        """,
        params_exact=[sector_p],
        params_fallback=kw_any_p,
    ).to_dict(orient="records")

    # ── Grants ─────────────────────────────────────────────────────────────
    grants = _query_with_fallback(
        con,
        sql_exact="""
            SELECT opp_id, title, agency, funding_max,
                   close_date::VARCHAR AS close_date,
                   posted_date::VARCHAR AS posted_date,
                   eligibility
            FROM unified_opportunities
            WHERE source = 'grants' AND sector ILIKE ?
              AND (close_date IS NULL OR close_date >= CURRENT_DATE)
            ORDER BY close_date ASC NULLS LAST
            LIMIT 5
        """,
        sql_fallback=f"""
            SELECT opp_id, title, agency, funding_max,
                   close_date::VARCHAR AS close_date,
                   posted_date::VARCHAR AS posted_date,
                   eligibility
            FROM unified_opportunities
            WHERE source = 'grants'
              AND (close_date IS NULL OR close_date >= CURRENT_DATE)
              AND {kw_any_cond}
            ORDER BY close_date ASC NULLS LAST
            LIMIT 5
        """,
        params_exact=[sector_p],
        params_fallback=kw_any_p,
    ).to_dict(orient="records")

    # ── Patents ────────────────────────────────────────────────────────────
    patents = _query_with_fallback(
        con,
        sql_exact="""
            SELECT opp_id, title, agency,
                   posted_date::VARCHAR AS posted_date,
                   array_to_string(tags, ',') AS tags
            FROM unified_opportunities
            WHERE source = 'patents' AND sector ILIKE ?
            ORDER BY posted_date DESC NULLS LAST
            LIMIT 5
        """,
        sql_fallback=f"""
            SELECT opp_id, title, agency,
                   posted_date::VARCHAR AS posted_date,
                   array_to_string(tags, ',') AS tags
            FROM unified_opportunities
            WHERE source = 'patents'
              AND {kw_title_cond}
            ORDER BY posted_date DESC NULLS LAST
            LIMIT 5
        """,
        params_exact=[sector_p],
        params_fallback=kw_title_p,
    ).to_dict(orient="records")

    # ── Research ───────────────────────────────────────────────────────────
    research = _query_with_fallback(
        con,
        sql_exact="""
            SELECT opp_id, title, agency,
                   posted_date::VARCHAR AS posted_date,
                   array_to_string(tags, ',') AS tags
            FROM unified_opportunities
            WHERE source = 'research' AND sector ILIKE ?
            ORDER BY posted_date DESC NULLS LAST
            LIMIT 5
        """,
        sql_fallback=f"""
            SELECT opp_id, title, agency,
                   posted_date::VARCHAR AS posted_date,
                   array_to_string(tags, ',') AS tags
            FROM unified_opportunities
            WHERE source = 'research'
              AND {kw_title_cond}
            ORDER BY posted_date DESC NULLS LAST
            LIMIT 5
        """,
        params_exact=[sector_p],
        params_fallback=kw_title_p,
    ).to_dict(orient="records")

    failures = _query_with_fallback(
        con,
        sql_exact="""
            SELECT failure_id, company_name, sector,
                   year_failed, funding_raised_usd,
                   failure_reasons, key_lesson, founder_names
            FROM failures_unified
            WHERE sector ILIKE ?
            ORDER BY funding_raised_usd DESC NULLS LAST
            LIMIT 5
        """,
        sql_fallback=f"""
            SELECT failure_id, company_name, sector,
                   year_failed, funding_raised_usd,
                   failure_reasons, key_lesson, founder_names
            FROM failures_unified
            WHERE ({kw_title_cond.replace('title', 'company_name')}
                   OR key_lesson ILIKE ?
                   OR CAST(raw_json->>'description' AS VARCHAR) ILIKE ?)
            ORDER BY funding_raised_usd DESC NULLS LAST
            LIMIT 5
        """,
        params_exact=[sector_p],
        params_fallback=kw_title_p + [f"%{kws[0] if kws else sector}%", f"%{kws[0] if kws else sector}%"],
    ).to_dict(orient="records")

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