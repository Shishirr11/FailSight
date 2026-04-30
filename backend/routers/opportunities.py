import json
import math
from fastapi import APIRouter, Query, HTTPException
from loguru import logger

from storage.db import get_db

router = APIRouter()

def _clean(record: dict) -> dict:
    import numpy as np
    out = {}
    for k, v in record.items():
        if isinstance(v, np.ndarray):
            v = v.tolist()
        elif hasattr(v, 'ndim') and v.ndim == 0 and hasattr(v, 'item'):
            v = v.item()
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            v = None
        elif isinstance(v, list):
            v = [x.item() if (hasattr(x, 'ndim') and x.ndim == 0) else x for x in v]
        elif v == '':
            v = None
        out[k] = v
    return out

def _clean_rows(rows: list[dict]) -> list[dict]:
    return [_clean(r) for r in rows]

@router.get("/stats")
def opportunity_stats():
    con = get_db()

    by_source = _clean_rows(
        con.execute("""
            SELECT source, COUNT(*)::INTEGER as count
            FROM unified_opportunities
            GROUP BY source
            ORDER BY count DESC
        """).fetchdf().to_dict(orient="records")
    )

    by_sector = _clean_rows(
        con.execute("""
            SELECT
                sector,
                COUNT(*)::INTEGER as count,
                COALESCE(
                    AVG(funding_max) FILTER (
                        WHERE funding_max IS NOT NULL AND funding_max > 0
                    ), 0
                )::DOUBLE as avg_funding
            FROM unified_opportunities
            WHERE sector IS NOT NULL
            GROUP BY sector
            ORDER BY count DESC
            LIMIT 10
        """).fetchdf().fillna(0).to_dict(orient="records")
    )

    total = int(
        con.execute("SELECT COUNT(*) FROM unified_opportunities").fetchone()[0]
    )

    return {
        "total":     total,
        "by_source": by_source,
        "by_sector": by_sector,
    }

@router.get("")
def list_opportunities(
    q:           str       = Query(None,
                                   description="Keyword search on title + description"),
    source:      list[str] = Query(None,
                                   description="sam | grants | patents | research"),
    sector:      str       = Query(None,  description="Filter by sector label"),
    agency:      str       = Query(None,  description="Filter by agency name"),
    naics:       str       = Query(None,  description="Filter by NAICS code prefix"),
    min_funding: float     = Query(None,  description="Minimum funding_max value"),
    max_funding: float     = Query(None,  description="Maximum funding_max value"),
    open_only:   bool      = Query(True,  description="Only return non-closed opps"),
    limit:       int       = Query(20,    ge=1, le=100),
    offset:      int       = Query(0,     ge=0),
):
    con    = get_db()
    wheres = []
    params = []

    if q:
        wheres.append("(title ILIKE ? OR description ILIKE ?)")
        params += [f"%{q}%", f"%{q}%"]

    if source:
        placeholders = ", ".join("?" for _ in source)
        wheres.append(f"source IN ({placeholders})")
        params += source

    if sector:
        wheres.append("sector ILIKE ?")
        params.append(f"%{sector}%")

    if agency:
        wheres.append("agency ILIKE ?")
        params.append(f"%{agency}%")

    if naics:
        wheres.append("naics_code LIKE ?")
        params.append(f"{naics}%")

    if min_funding is not None:
        wheres.append("funding_max >= ?")
        params.append(min_funding)

    if max_funding is not None:
        wheres.append("funding_max <= ?")
        params.append(max_funding)

    if open_only:
        wheres.append("(close_date IS NULL OR close_date >= CURRENT_DATE)")

    where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""

    total = int(
        con.execute(
            f"SELECT COUNT(*) FROM unified_opportunities {where_clause}", params
        ).fetchone()[0]
    )

    rows = con.execute(f"""
        SELECT
            opp_id, source, title, sector, agency,
            naics_code, geography, eligibility,
            funding_min, funding_max,
            posted_date::VARCHAR  as posted_date,
            close_date::VARCHAR   as close_date,
            array_to_string(tags, ',') as tags
        FROM unified_opportunities
        {where_clause}
        ORDER BY posted_date DESC NULLS LAST
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchdf()

    return {
        "total":   total,
        "limit":   limit,
        "offset":  offset,
        "results": _clean_rows(rows.fillna("").to_dict(orient="records")),
    }

@router.get("/{opp_id}")
def get_opportunity(opp_id: str):
    con = get_db()
    row = con.execute(
        """
        SELECT
            opp_id, source, title, description, sector, naics_code,
            agency, geography, eligibility, tags,
            funding_min, funding_max,
            posted_date::VARCHAR as posted_date,
            close_date::VARCHAR  as close_date,
            raw_json
        FROM unified_opportunities
        WHERE opp_id = ?
        """,
        [opp_id]
    ).fetchdf()

    if row.empty:
        raise HTTPException(status_code=404,
                            detail=f"Opportunity '{opp_id}' not found.")

    record = _clean(row.fillna("").to_dict(orient="records")[0])

    try:
        raw = json.loads(record.get("raw_json") or "{}")

        for field in ("cfda_numbers", "eligibilities", "cost_sharing",
                      "solicitation_number", "notice_type", "contact",
                      "resource_links", "ui_link"):
            if field not in record:
                record[field] = raw.get(field)
    except Exception:
        pass

    enr = con.execute("""
        SELECT full_text, summary, key_fields, enrichment_status, source AS enr_source
        FROM enriched_details
        WHERE record_id = ?
        ORDER BY enriched_at DESC
        LIMIT 1
    """, [opp_id]).fetchdf()

    if not enr.empty:
        enr_row = enr.fillna("").to_dict(orient="records")[0]
        record["full_text"]          = enr_row.get("full_text", "")
        record["summary"]            = enr_row.get("summary", "")
        record["enrichment_status"]  = enr_row.get("enrichment_status", "")

        try:
            kf = json.loads(enr_row.get("key_fields") or "{}")
            record["key_fields"] = kf

            for promoted in (
                "doi", "authors", "institutions", "citation_count",
                "open_access_url", "patent_id", "assignees", "inventors",
                "cpc_codes", "cpc_titles", "filing_date",
                "cfda_numbers", "eligibilities", "notice_type",
                "solicitation_number", "contact", "resource_links",
            ):
                if promoted not in record and promoted in kf:
                    record[promoted] = kf[promoted]
        except Exception:
            record["key_fields"] = {}

    return record
