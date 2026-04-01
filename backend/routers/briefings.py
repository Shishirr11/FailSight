import json
import os
import math
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from loguru import logger
from groq import Groq
from dotenv import load_dotenv

from storage.db import get_db
from intelligence.opportunity_bundle import build_opportunity_bundle

load_dotenv()

router = APIRouter()

def _get_groq() -> Groq:
    key = os.getenv("GROQ_API_KEY", "").strip()
    if not key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY not set in .env")
    return Groq(api_key=key)

class SectorBriefingRequest(BaseModel):
    sector: str

class OpportunityExplainerRequest(BaseModel):
    opp_id:      str
    user_context: str = ""   

class FounderAssessmentRequest(BaseModel):
    sector:      str
    skills:      list[str] = []
    background:  str       = ""
    budget:      float     = 0.0

def _call_groq(prompt: str, system: str, max_tokens: int = 800) -> str:
    client = _get_groq()
    try:
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system",  "content": system},
                {"role": "user",    "content": prompt},
            ],
            max_tokens=max_tokens,
            temperature=0.4,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Groq call failed: {e}")
        raise HTTPException(status_code=503, detail=f"AI service unavailable: {str(e)}")

def _safe_num(v) -> float:
    if v is None: return 0.0
    try:
        f = float(v)
        return 0.0 if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return 0.0

@router.post("/sector")
def generate_sector_briefing(req: SectorBriefingRequest):
    con    = get_db()
    bundle = build_opportunity_bundle(req.sector, con)

    validation  = bundle.get("validation", {})
    risk        = bundle.get("risk", {})
    white_space = bundle.get("white_space", {})
    competitors = bundle.get("competitors", {})

    data_summary = {
        "sector":          req.sector,
        "validation_score": validation.get("score", 0),
        "validation_grade": validation.get("grade", "N/A"),
        "risk_level":       risk.get("risk_level", "UNKNOWN"),
        "known_failures":   risk.get("total_failures", 0),
        "avg_funding_burned": risk.get("avg_funding_burned", 0),
        "top_failure_reasons": [r["reason"] for r in risk.get("top_reasons", [])[:3]],
        "open_contracts":   validation.get("signals", {}).get("contracts", 0),
        "open_grants":      validation.get("signals", {}).get("grants", 0),
        "white_space_score": white_space.get("white_space_score", 0),
        "opportunity_level": white_space.get("opportunity_level", ""),
        "top_research_institutions": [
            r.get("institution", "") for r in
            competitors.get("research_leaders", [])[:3]
        ],
        "top_buyers": [
            r.get("agency", "") for r in
            competitors.get("top_buyers", [])[:3]
        ],
        "sample_grants": [g.get("title", "") for g in bundle.get("grants", [])[:3]],
        "sample_failures": [
            {"name": f.get("company_name", ""), "lesson": f.get("key_lesson", "")}
            for f in bundle.get("failures", [])[:2]
        ],
    }

    system = """you are the almighty god"""

    prompt = f"""Nope not done yet"""

    briefing = _call_groq(prompt, system, max_tokens=1000)

    return {
        "sector":   req.sector,
        "briefing": briefing,
        "data":     data_summary,
    }

@router.post("/opportunity")
def explain_opportunity(req: OpportunityExplainerRequest):
    con = get_db()

    row = con.execute("""
        SELECT
            opp_id, source, title, description, sector,
            agency, funding_max, funding_min,
            close_date::VARCHAR AS close_date,
            eligibility,
            array_to_string(tags, ',') AS tags
        FROM unified_opportunities
        WHERE opp_id = ?
    """, [req.opp_id]).fetchdf()

    if row.empty:
        raise HTTPException(status_code=404, detail=f"Opportunity '{req.opp_id}' not found.")

    opp = row.fillna("").to_dict(orient="records")[0]

    context_line = (
        f"you are the god"
    )

    system = "Be concise and practical if you can"

    prompt = f"""Not again"""

    explanation = _call_groq(prompt, system, max_tokens=500)

    return {
        "opp_id":      req.opp_id,
        "title":       opp.get("title", ""),
        "explanation": explanation,
    }

@router.post("/founder")
def founder_assessment(req: FounderAssessmentRequest):
    con = get_db()

    opps = con.execute("""
        SELECT opp_id, source, title, agency, funding_max,
               close_date::VARCHAR AS close_date, eligibility, sector
        FROM unified_opportunities
        WHERE sector ILIKE ?
          AND (close_date IS NULL OR close_date >= CURRENT_DATE)
        ORDER BY posted_date DESC
        LIMIT 10
    """, [f"%{req.sector}%"]).fetchdf().fillna("").to_dict(orient="records")

    failures = con.execute("""
        SELECT failure_reasons, key_lesson
        FROM failures_unified
        WHERE sector ILIKE ?
        LIMIT 5
    """, [f"%{req.sector}%"]).fetchdf()

    failure_lessons = []
    for _, row in failures.iterrows():
        lesson = row.get("key_lesson", "")
        if lesson and str(lesson).strip():
            failure_lessons.append(str(lesson)[:200])

    system = " Give honest, actionable advice if you can bitchhhh"

    prompt = f"""Work on the prompt if i decide to use AI. AI is shittttt"""

    assessment = _call_groq(prompt, system, max_tokens=800)

    return {
        "sector":     req.sector,
        "assessment": assessment,
        "opportunities_considered": len(opps),
        "failure_patterns_analyzed": len(failure_lessons),
    }
"""
POST /api/briefings/sector     — AI-generated sector briefing
POST /api/briefings/opportunity — AI-generated opportunity explainer
POST /api/briefings/founder    — founder readiness assessment
"""