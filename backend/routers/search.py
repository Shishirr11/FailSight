import json
import math
import os
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from loguru import logger
from groq import Groq
from dotenv import load_dotenv

from storage.db import get_db

load_dotenv()

router = APIRouter()

def _get_groq_client() -> Groq:
    key = os.getenv("GROQ_API_KEY", "").strip()
    if not key:
        raise HTTPException(
            status_code=500,
            detail="GROQ_API_KEY not set in .env"
        )
    return Groq(api_key=key)


class SearchRequest(BaseModel):
    query:      str
    sources:    list[str] = []          
    limit:      int       = 20
    offset:     int       = 0


class SearchResponse(BaseModel):
    query:           str
    interpreted_as:  dict               
    total:           int
    results:         list[dict]

SYSTEM_PROMPT = """work on the prompt idiot
"""


def interpret_query(query: str) -> dict:

    client = _get_groq_client()

    try:
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": query},
            ],
            max_tokens=300,
            temperature=0.1,    
        )
        raw = resp.choices[0].message.content.strip()

        raw = raw.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(raw)

        return {
            "keywords":       parsed.get("keywords", []),
            "sectors":        parsed.get("sectors", []),
            "intent":         parsed.get("intent", query),
            "min_funding":    parsed.get("min_funding"),
            "max_funding":    parsed.get("max_funding"),
            "sources":        parsed.get("sources", []),
            "time_sensitive": parsed.get("time_sensitive", False),
        }

    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"Groq interpretation failed: {e} — falling back to keyword search")
        return {
            "keywords":       query.split()[:5],
            "sectors":        [],
            "intent":         query,
            "min_funding":    None,
            "max_funding":    None,
            "sources":        [],
            "time_sensitive": False,
        }

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


def search_opportunities(interpreted: dict, sources_override: list[str],
                         limit: int, offset: int, con) -> tuple[int, list[dict]]:

    keywords = interpreted.get("keywords", [])
    sectors  = interpreted.get("sectors", [])
    sources  = sources_override or interpreted.get("sources", [])
    min_fund = interpreted.get("min_funding")
    max_fund = interpreted.get("max_funding")

    wheres = []
    params = []
    if keywords:
        kw_clauses = []
        for kw in keywords:
            kw_clauses.append("(title ILIKE ? OR description ILIKE ?)")
            params += [f"%{kw}%", f"%{kw}%"]
        wheres.append("(" + " OR ".join(kw_clauses) + ")")

    if sectors:
        sector_clauses = " OR ".join("sector ILIKE ?" for _ in sectors)
        wheres.append(f"({sector_clauses})")
        params += [f"%{s}%" for s in sectors]

    if sources:
        placeholders = ", ".join("?" for _ in sources)
        wheres.append(f"source IN ({placeholders})")
        params += sources

    if min_fund is not None:
        wheres.append("funding_max >= ?")
        params.append(min_fund)
    if max_fund is not None:
        wheres.append("funding_max <= ?")
        params.append(max_fund)

    wheres.append("(close_date IS NULL OR close_date >= CURRENT_DATE)")

    where_clause = "WHERE " + " AND ".join(wheres) if wheres else ""
    relevance_parts = []
    relevance_params = []
    for kw in keywords:
        relevance_parts.append("(CASE WHEN title ILIKE ? THEN 2 ELSE 0 END)")
        relevance_params.append(f"%{kw}%")
        relevance_parts.append("(CASE WHEN description ILIKE ? THEN 1 ELSE 0 END)")
        relevance_params.append(f"%{kw}%")

    relevance_expr = (
        "(" + " + ".join(relevance_parts) + ") AS relevance_score"
        if relevance_parts else "1 AS relevance_score"
    )

    total = int(con.execute(
        f"SELECT COUNT(*) FROM unified_opportunities {where_clause}", params
    ).fetchone()[0])

    rows = con.execute(f"""
        SELECT
            opp_id, source, title, sector, agency,
            funding_min, funding_max,
            posted_date::VARCHAR  AS posted_date,
            close_date::VARCHAR   AS close_date,
            eligibility,
            array_to_string(tags, ',') AS tags,
            {relevance_expr}
        FROM unified_opportunities
        {where_clause}
        ORDER BY relevance_score DESC, posted_date DESC NULLS LAST
        LIMIT ? OFFSET ?
    """, relevance_params + params + [limit, offset]).fetchdf()

    return total, [_clean(r) for r in rows.fillna("").to_dict(orient="records")]

@router.post("")
def semantic_search(req: SearchRequest):
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    logger.info(f"Search query: '{req.query}'")
    interpreted = interpret_query(req.query)
    logger.info(f"Interpreted as: {interpreted}")
    con = get_db()
    total, results = search_opportunities(
        interpreted, req.sources, req.limit, req.offset, con
    )

    return {
        "query":          req.query,
        "interpreted_as": interpreted,
        "total":          total,
        "results":        results,
    }


@router.get("/suggest")
def suggest(q: str = "") -> dict:
    if len(q) < 2:
        return {"sectors": [], "agencies": []}

    con = get_db()

    sectors = con.execute("""
        SELECT DISTINCT sector
        FROM unified_opportunities
        WHERE sector ILIKE ? AND sector IS NOT NULL
        ORDER BY sector LIMIT 5
    """, [f"%{q}%"]).fetchall()

    agencies = con.execute("""
        SELECT DISTINCT agency
        FROM unified_opportunities
        WHERE agency ILIKE ? AND agency IS NOT NULL AND agency != ''
        ORDER BY agency LIMIT 5
    """, [f"%{q}%"]).fetchall()

    return {
        "sectors":  [r[0] for r in sectors],
        "agencies": [r[0] for r in agencies],
    }
"""
POST /api/search          — Groq
GET  /api/search/suggest  — autocomplete 
"""
