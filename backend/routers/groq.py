import os
import json
import math
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from loguru import logger
from groq import Groq

from storage.db import get_db

router = APIRouter()


def _groq() -> Groq:
    key = os.getenv("GROQ_API_KEY", "").strip()
    if not key:
        raise HTTPException(
            status_code=503,
            detail="GROQ_API_KEY not configured. Add it to your .env file.",
        )
    return Groq(api_key=key)


GROQ_MODEL = "llama-3.3-70b-versatile"  


def _chat(system: str, user: str, max_tokens: int = 400) -> str:
    client = _groq()
    resp = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        max_tokens=max_tokens,
        temperature=0.4,
    )
    return resp.choices[0].message.content.strip()


def _clean(v):
    import numpy as np
    if isinstance(v, np.ndarray):
        return [_clean(x) for x in v.tolist()]
    if hasattr(v, "ndim") and v.ndim == 0 and hasattr(v, "item"):
        v = v.item()
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, dict):
        return {k: _clean(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_clean(x) for x in v]
    return v



class WhyCareRequest(BaseModel):
    opp_id: str
    user_query: str        


@router.post("/why-care")
def why_care(req: WhyCareRequest):
    con = get_db()

    row = con.execute("""
        SELECT
            opp_id, source, title, description, sector,
            agency, funding_max, funding_min,
            close_date::VARCHAR  AS close_date,
            array_to_string(tags, ',') AS tags
        FROM unified_opportunities
        WHERE opp_id = ?
    """, [req.opp_id]).fetchdf()

    if row.empty:
        raise HTTPException(status_code=404, detail=f"Opportunity '{req.opp_id}' not found.")

    opp = _clean(row.fillna("").to_dict(orient="records")[0])

    funding = opp.get("funding_max") or opp.get("funding_min")
    funding_str = f"${funding:,.0f}" if funding else "amount not specified"

    system = (
        "You are a concise startup intelligence assistant. "
        "Your only job is to write 2-3 tight sentences explaining why a specific funding opportunity "
        "is relevant to a founder's stated interest. "
        "Be specific — mention the agency, funding amount, and sector. "
        "No bullet points. No preamble. Just the sentences."
    )

    user = f"""
The founder searched for: "{req.user_query}"

Here is the opportunity they clicked on:
- Title: {opp.get('title', '')}
- Source: {opp.get('source', '').upper()}
- Agency: {opp.get('agency', 'Unknown')}
- Sector: {opp.get('sector', 'Unknown')}
- Funding: {funding_str}
- Closes: {opp.get('close_date') or 'Open / Ongoing'}
- Description: {str(opp.get('description') or '')[:500]}
- Tags: {opp.get('tags', '')}

Write 2-3 sentences explaining why this opportunity is relevant to what they searched for.
Be specific and human. Don't start with "This opportunity" — vary the opening, starting like Your search relates to this by......
"""

    explanation = _chat(system, user, max_tokens=200)

    return {
        "opp_id":      req.opp_id,
        "user_query":  req.user_query,
        "title":       opp.get("title", ""),
        "explanation": explanation,
    }


ALL_SECTORS = [
    "AI & Machine Learning", "Cybersecurity", "Clean Energy", "Climate Technology",
    "Biotechnology", "Health Technology", "Quantum Computing", "Advanced Manufacturing",
    "Aerospace & Defense", "Agriculture Technology", "Advanced Computing", "Fintech",
    "Transportation", "Infrastructure", "Education", "Small Business", "Community Development",
]

class SectorSuggestRequest(BaseModel):
    input: str


@router.post("/sector-suggest")
def sector_suggest(req: SectorSuggestRequest):
    user_input = req.input.strip()
    if not user_input or len(user_input) < 2:
        return {"sectors": []}

    substring_matches = [
        s for s in ALL_SECTORS
        if user_input.lower() in s.lower()
    ]

    if len(user_input) < 3:
        return {"sectors": substring_matches[:5]}

    system = (
        "You are a sector classification assistant. "
        "Given a user's input text, return the 3-5 most semantically relevant sectors "
        "from the provided list. "
        "Respond ONLY with a JSON array of sector name strings — nothing else. "
        "Example: [\"AI & Machine Learning\", \"Health Technology\"]"
    )

    user = f"""
User typed: "{user_input}"

Available sectors:
{chr(10).join(f'- {s}' for s in ALL_SECTORS)}

Return the 3-5 most relevant sectors as a JSON array.
"""

    try:
        raw = _chat(system, user, max_tokens=100)
        cleaned = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        sectors = json.loads(cleaned)
        valid = [s for s in sectors if s in ALL_SECTORS]
        if valid:
            return {"sectors": valid[:5]}
    except Exception:
        pass

    return {"sectors": substring_matches[:5] or ALL_SECTORS[:5]}

class ValidateIdeaRequest(BaseModel):
    idea: str                          
    sector: Optional[str] = None     


@router.post("/validate-idea")
def validate_idea(req: ValidateIdeaRequest):
    con = get_db()
    sector_filter = f"%{req.sector}%" if req.sector else "%"
    idea_kw = " ".join(req.idea.split()[:6])  
    grants_df = con.execute("""
        SELECT title, agency, funding_max, close_date::VARCHAR AS close_date, eligibility
        FROM unified_opportunities
        WHERE source = 'grants'
          AND (close_date IS NULL OR close_date >= CURRENT_DATE)
          AND (title ILIKE ? OR description ILIKE ?)
        ORDER BY funding_max DESC NULLS LAST
        LIMIT 5
    """, [f"%{idea_kw}%", f"%{idea_kw}%"]).fetchdf()
    if grants_df.empty and req.sector:
        grants_df = con.execute("""
            SELECT title, agency, funding_max, close_date::VARCHAR AS close_date, eligibility
            FROM unified_opportunities
            WHERE source = 'grants'
              AND (close_date IS NULL OR close_date >= CURRENT_DATE)
              AND sector ILIKE ?
            ORDER BY funding_max DESC NULLS LAST
            LIMIT 5
        """, [sector_filter]).fetchdf()

    grants = _clean(grants_df.fillna("").to_dict(orient="records"))

    failures_df = con.execute("""
        SELECT company_name, year_failed, funding_raised_usd, failure_reasons, key_lesson
        FROM failures_unified
        WHERE sector ILIKE ?
        ORDER BY funding_raised_usd DESC NULLS LAST
        LIMIT 5
    """, [sector_filter]).fetchdf()
    failures = _clean(failures_df.fillna("").to_dict(orient="records"))

    market_row = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE source IN ('sam','grants'))  AS market_signal,
            COUNT(*) FILTER (WHERE source IN ('patents','research')) AS innovation_signal
        FROM unified_opportunities
        WHERE sector ILIKE ?
    """, [sector_filter]).fetchone()
    market_signal     = int(market_row[0]) if market_row else 0
    innovation_signal = int(market_row[1]) if market_row else 0

    grants_txt = "\n".join(
        f"  - {g.get('title','')} | {g.get('agency','')} | "
        f"${g.get('funding_max') or 0:,.0f} | closes {g.get('close_date') or 'ongoing'}"
        for g in grants[:5]
    ) or "  No matching grants found in database."

    failures_txt = "\n".join(
        f"  - {f.get('company_name','')} ({f.get('year_failed','?')}): "
        f"{', '.join(r for r in (f.get('failure_reasons') or []) if isinstance(r, str)) or 'unknown reasons'}"
        for f in failures[:5]
    ) or "  No known failures in this area."

    system = (
        "You are a blunt, data-driven startup advisor. "
        "You read real grant data, failure post-mortems, and market signals, then give a fast, honest verdict. "
        "Respond ONLY with a valid JSON object — no markdown, no explanation outside JSON. "
        "Keys: market_exists (bool), confidence (LOW|MEDIUM|HIGH), verdict (string, 2 sentences max), "
        "biggest_risk (string, 1 sentence), first_grant (string — exact grant title or 'None found')."
    )

    user = f"""
Startup idea: "{req.idea}"

Real data from our database:

OPEN GRANTS that match:
{grants_txt}

KNOWN FAILURES in this space:
{failures_txt}

MARKET SIGNALS:
- Active government contracts/grants: {market_signal}
- Active research/patents: {innovation_signal}

Based on this data, give your verdict.
Remember: respond only with the JSON object.
"""

    raw = _chat(system, user, max_tokens=350)
    try:
        cleaned = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        result  = json.loads(cleaned)
    except Exception:
        logger.warning(f"validate-idea: JSON parse failed — raw: {raw[:200]}")
        result = {
            "market_exists": None,
            "confidence":    "LOW",
            "verdict":       raw[:300],
            "biggest_risk":  "Unable to parse structured response.",
            "first_grant":   grants[0].get("title") if grants else "None found",
        }

    result["matching_grants"] = grants[:3]
    result["known_failures"]  = failures[:3]

    return result

class GapFinderRequest(BaseModel):
    sector: str


@router.post("/gap-finder")
def gap_finder(req: GapFinderRequest):
    con = get_db()
    sector = req.sector

    ws = con.execute("""
        WITH signals AS (
            SELECT
                SUM(CASE WHEN source IN ('patents','research') THEN 1 ELSE 0 END) AS innovation,
                SUM(CASE WHEN source IN ('sam','grants')       THEN 1 ELSE 0 END) AS market
            FROM unified_opportunities
            WHERE sector ILIKE ?
        )
        SELECT innovation, market, (innovation - market) AS gap_score FROM signals
    """, [f"%{sector}%"]).fetchone()
    innovation = int(ws[0] or 0) if ws else 0
    market     = int(ws[1] or 0) if ws else 0
    gap_score  = int(ws[2] or 0) if ws else 0

    research_df = con.execute("""
        SELECT title, agency, posted_date::VARCHAR AS posted_date
        FROM unified_opportunities
        WHERE source IN ('patents','research') AND sector ILIKE ?
        ORDER BY posted_date DESC NULLS LAST
        LIMIT 10
    """, [f"%{sector}%"]).fetchdf()
    research_items = _clean(research_df.fillna("").to_dict(orient="records"))

    contracts_df = con.execute("""
        SELECT title, agency, funding_max, close_date::VARCHAR AS close_date
        FROM unified_opportunities
        WHERE source = 'sam' AND sector ILIKE ?
          AND (close_date IS NULL OR close_date >= CURRENT_DATE)
        ORDER BY funding_max DESC NULLS LAST
        LIMIT 8
    """, [f"%{sector}%"]).fetchdf()
    contracts = _clean(contracts_df.fillna("").to_dict(orient="records"))

    failures_df = con.execute("""
        SELECT company_name, year_failed, failure_reasons, key_lesson
        FROM failures_unified
        WHERE sector ILIKE ?
        ORDER BY year_failed DESC NULLS LAST
        LIMIT 10
    """, [f"%{sector}%"]).fetchdf()
    failures = _clean(failures_df.fillna("").to_dict(orient="records"))

    research_txt = "\n".join(
        f"  - [{r.get('posted_date','?')}] {r.get('title','')}"
        for r in research_items
    ) or "  No recent research found."

    contracts_txt = "\n".join(
        f"  - {c.get('title','')} | {c.get('agency','')} | "
        f"${c.get('funding_max') or 0:,.0f} | closes {c.get('close_date') or 'open'}"
        for c in contracts
    ) or "  No open contracts found."

    failures_txt = "\n".join(
        f"  - {f.get('company_name','?')} ({f.get('year_failed','?')}): "
        f"{', '.join(r for r in (f.get('failure_reasons') or []) if isinstance(r, str)) or 'unknown'}"
        f" — lesson: {str(f.get('key_lesson',''))[:120]}"
        for f in failures
    ) or "  No known failures on record."

    system = (
        "You are a startup market analyst who finds non-obvious opportunities by reading signals "
        "from government data, research papers, and failure post-mortems. "
        "Your job is to surface the white space — what hasn't been built yet, what the market is "
        "quietly demanding, and what founders keep getting wrong. "
        "Be concrete and specific. Avoid generic advice. "
        "Respond ONLY with a JSON object — no markdown fences. "
        "Keys: tried_and_failed (string), researched_not_funded (string), "
        "open_demand (string), non_obvious_opportunity (string)."
    )

    user = f"""
Sector: {sector}

White-space score: {gap_score} (innovation={innovation} signals, market={market} signals)

RECENT RESEARCH & PATENTS (what's being studied):
{research_txt}

OPEN GOVERNMENT CONTRACTS (real buyer demand):
{contracts_txt}

KNOWN STARTUP FAILURES (what's been tried & killed):
{failures_txt}

Based on this data, identify:
1. What has been tried and repeatedly failed, and why
2. What is being actively researched but has no funded products yet
3. Where the open contracts show real unmet demand
4. One non-obvious opportunity a founder browsing this data wouldn't immediately notice

Respond only with the JSON object.
"""

    raw = _chat(system, user, max_tokens=600)

    try:
        cleaned = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        analysis = json.loads(cleaned)
    except Exception:
        logger.warning(f"gap-finder: JSON parse failed — {raw[:200]}")
        analysis = {
            "tried_and_failed":        "Unable to parse response.",
            "researched_not_funded":   raw[:300],
            "open_demand":             "",
            "non_obvious_opportunity": "",
        }

    return {
        "sector":       sector,
        "signals": {
            "innovation": innovation,
            "market":     market,
            "gap_score":  gap_score,
        },
        "analysis":   analysis,
        "research":   research_items[:5],
        "contracts":  contracts[:5],
        "failures":   failures[:5],
    }


class GrantMatchRequest(BaseModel):
    project_description: str
    sector: Optional[str] = None


@router.post("/grant-match")
def grant_match(req: GrantMatchRequest):
   
    con = get_db()
    kw = " ".join(req.project_description.split()[:8])
    sector_filter = f"%{req.sector}%" if req.sector else "%"
    grants_df = con.execute("""
        SELECT
            opp_id, title, agency, funding_max, funding_min,
            close_date::VARCHAR AS close_date,
            eligibility, description,
            array_to_string(tags, ',') AS tags
        FROM unified_opportunities
        WHERE source = 'grants'
          AND (close_date IS NULL OR close_date >= CURRENT_DATE)
          AND sector ILIKE ?
          AND (title ILIKE ? OR description ILIKE ?)
        ORDER BY funding_max DESC NULLS LAST
        LIMIT 5
    """, [sector_filter, f"%{kw}%", f"%{kw}%"]).fetchdf()

    if grants_df.empty:
        grants_df = con.execute("""
            SELECT
                opp_id, title, agency, funding_max, funding_min,
                close_date::VARCHAR AS close_date,
                eligibility, description,
                array_to_string(tags, ',') AS tags
            FROM unified_opportunities
            WHERE source = 'grants'
              AND (close_date IS NULL OR close_date >= CURRENT_DATE)
              AND sector ILIKE ?
            ORDER BY funding_max DESC NULLS LAST
            LIMIT 5
        """, [sector_filter]).fetchdf()

    if grants_df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No open grants found for sector '{req.sector or 'any'}'. "
                   "Try a broader sector or remove the sector filter.",
        )

    grants = _clean(grants_df.fillna("").to_dict(orient="records"))

    grants_txt = "\n\n".join(
        f"GRANT {i+1}: {g.get('title','')}\n"
        f"  Agency:      {g.get('agency','Unknown')}\n"
        f"  Funding:     ${g.get('funding_max') or g.get('funding_min') or 0:,.0f}\n"
        f"  Closes:      {g.get('close_date') or 'ongoing'}\n"
        f"  Eligibility: {str(g.get('eligibility') or '')[:300]}\n"
        f"  Description: {str(g.get('description') or '')[:400]}"
        for i, g in enumerate(grants)
    )

    system = (
        "You are a grant-writing advisor. You read a project description and compare it against "
        "real grant opportunities to find the best fit. "
        "Be blunt about gaps. Be specific about what criteria are met. "
        "Respond ONLY with a valid JSON object — no markdown. "
        "Keys: best_match (string — exact grant title), fit_score (integer 1-10), "
        "met_criteria (list of strings), missing (list of strings), "
        "ranked_grants (list of objects with keys: title, fit_note)."
    )

    user = f"""
PROJECT DESCRIPTION:
\"\"\"{req.project_description}\"\"\"

AVAILABLE GRANTS (top 5):
{grants_txt}

Analyse the project description against each grant.
Identify which grant is the best fit and score it 1-10.
List the eligibility criteria the description already meets.
List what's missing from the description that would help the application.
Rank all grants with a one-line fit note each.

Respond only with the JSON object.
"""

    raw = _chat(system, user, max_tokens=700)

    try:
        cleaned = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        result  = json.loads(cleaned)
    except Exception:
        logger.warning(f"grant-match: JSON parse failed — {raw[:200]}")
        result = {
            "best_match":    grants[0].get("title", "Unknown") if grants else "None",
            "fit_score":     None,
            "met_criteria":  [],
            "missing":       ["Unable to parse structured response."],
            "ranked_grants": [{"title": g.get("title",""), "fit_note": ""} for g in grants],
        }
    result["grants"] = [
        {
            "opp_id":   g.get("opp_id"),
            "title":    g.get("title"),
            "agency":   g.get("agency"),
            "funding":  g.get("funding_max") or g.get("funding_min"),
            "closes":   g.get("close_date"),
        }
        for g in grants
    ]

    return result