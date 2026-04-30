import json
import math
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from loguru import logger

from storage.db import get_db
from intelligence.opportunity_bundle import build_opportunity_bundle

router = APIRouter()


class SectorBriefingRequest(BaseModel):
    sector: str


class OpportunityExplainerRequest(BaseModel):
    opp_id:       str
    user_context: str = ""



def _safe_float(v) -> float:
    if v is None:
        return 0.0
    try:
        f = float(v)
        return 0.0 if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return 0.0


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


@router.post("/sector")
def generate_sector_briefing(req: SectorBriefingRequest):

    con    = get_db()
    bundle = build_opportunity_bundle(req.sector, con)

    validation  = bundle.get("validation",  {})
    risk        = bundle.get("risk",        {})
    white_space = bundle.get("white_space", {})
    competitors = bundle.get("competitors", {})
    grants      = bundle.get("grants",      [])
    failures    = bundle.get("failures",    [])

    score = validation.get("score", 0)
    grade = validation.get("grade", "?")
    risk_level    = risk.get("risk_level", "UNKNOWN")
    known_fails   = risk.get("total_failures", 0)
    ws_level      = white_space.get("opportunity_level", "")
    ws_score      = white_space.get("white_space_score", 0)
    open_contracts = validation.get("signals", {}).get("contracts", 0)
    open_grants    = validation.get("signals", {}).get("grants", 0)

    grade_desc = {
        "A": "Strong signals — active government demand, real budget being spent.",
        "B": "Moderate signals — some demand, worth validating further.",
        "C": "Weak signals — limited public sector activity in this area.",
        "D": "Very weak signals — little to no government funding evidence.",
    }.get(grade, "Insufficient data to grade.")

    risk_desc = {
        "LOW":    f"Low failure risk — only {known_fails} known failures in this sector.",
        "MEDIUM": f"Medium risk — {known_fails} startups failed here; tread carefully.",
        "HIGH":   f"High risk — {known_fails} documented failures. Study them before building.",
        "UNKNOWN": "Risk data insufficient — not enough failures on record.",
    }.get(risk_level, "")

    ws_desc = {
        "HIGH":     "High white space — strong R&D activity without matching market contracts. Classic first-mover opportunity.",
        "MODERATE": "Moderate white space — R&D slightly ahead of contracts. Monitor closely.",
        "LOW":      "Low white space — market and innovation activity are balanced. Competitive but established.",
    }.get(ws_level, "")

    top_reasons = [r["reason"] for r in risk.get("top_reasons", [])[:3]]
    top_buyers  = [r.get("agency", "") for r in competitors.get("top_buyers", [])[:3]]

    summary_lines = [
        f"Grade {grade}: {grade_desc}",
        risk_desc,
        ws_desc,
    ]
    if top_reasons:
        summary_lines.append(f"Top failure reasons in this sector: {', '.join(top_reasons)}.")
    if top_buyers:
        summary_lines.append(f"Biggest buyers: {', '.join(b for b in top_buyers if b)}.")
    if open_contracts:
        summary_lines.append(f"{open_contracts} open contracts right now — real procurement happening.")
    if open_grants:
        summary_lines.append(f"{open_grants} active grants available.")

    summary_text = " ".join(summary_lines)

    return {
        "sector":      req.sector,
        "summary":     summary_text,
        "validation":  validation,
        "risk":        risk,
        "white_space": white_space,
        "competitors": competitors,
        "grants":      [_clean(g) for g in grants[:5]],
        "failures":    [_clean(f) for f in failures[:5]],
    }


@router.post("/opportunity")
def explain_opportunity(req: OpportunityExplainerRequest):
    con = get_db()

    row = con.execute("""
        SELECT
            opp_id, source, title, description, sector,
            agency, funding_max, funding_min,
            close_date::VARCHAR  AS close_date,
            posted_date::VARCHAR AS posted_date,
            eligibility,
            array_to_string(tags, ',') AS tags
        FROM unified_opportunities
        WHERE opp_id = ?
    """, [req.opp_id]).fetchdf()

    if row.empty:
        raise HTTPException(status_code=404, detail=f"Opportunity '{req.opp_id}' not found.")

    opp = _clean(row.fillna("").to_dict(orient="records")[0])

    enr = con.execute("""
        SELECT full_text, summary, key_fields, enrichment_status
        FROM enriched_details
        WHERE record_id = ?
        LIMIT 1
    """, [req.opp_id]).fetchdf()

    enr_data = {}
    if not enr.empty:
        enr_row = enr.fillna("").to_dict(orient="records")[0]
        try:
            key_fields = json.loads(enr_row.get("key_fields") or "{}")
        except Exception:
            key_fields = {}
        enr_data = {
            "summary":    enr_row.get("summary", ""),
            "full_text":  (enr_row.get("full_text") or "")[:3000],
            "key_fields": key_fields,
            "status":     enr_row.get("enrichment_status", ""),
        }


    sector = opp.get("sector") or ""
    sector_stats = {}
    if sector:
        total_in_sector = con.execute("""
            SELECT COUNT(*) FROM unified_opportunities WHERE sector ILIKE ?
        """, [f"%{sector}%"]).fetchone()[0]

        failures_in_sector = con.execute("""
            SELECT COUNT(*) FROM failures_unified WHERE sector ILIKE ?
        """, [f"%{sector}%"]).fetchone()[0]

        sector_stats = {
            "opportunities_in_sector": int(total_in_sector),
            "failures_in_sector":      int(failures_in_sector),
        }
    related_failures = []
    if sector:
        fail_rows = con.execute("""
            SELECT company_name, year_failed, funding_raised_usd, key_lesson
            FROM failures_unified
            WHERE sector ILIKE ?
            ORDER BY funding_raised_usd DESC NULLS LAST
            LIMIT 3
        """, [f"%{sector}%"]).fetchdf()

        for _, r in fail_rows.iterrows():
            d = r.to_dict()
            related_failures.append({
                "company_name": d.get("company_name", ""),
                "year_failed":  d.get("year_failed"),
                "funding":      d.get("funding_raised_usd"),
                "lesson":       d.get("key_lesson", ""),
            })

    return {
        "opportunity": opp,
        "enrichment":  enr_data,
        "sector_stats": sector_stats,
        "related_failures": related_failures,
    }

"""
  POST /api/briefings/sector    
  POST /api/briefings/opportunity 
"""