import json
import math
from collections import Counter
from fastapi import APIRouter, Query, HTTPException
from loguru import logger

from storage.db import get_db

router = APIRouter()

def _clean(record: dict) -> dict:
    out = {}
    for k, v in record.items():
        if hasattr(v, "tolist"):
            v = v.tolist()
        elif isinstance(v, list):
            v = [x.item() if (hasattr(x, "item") and hasattr(x, "ndim") and x.ndim == 0)
                 else x for x in v]
        elif hasattr(v, "item") and hasattr(v, "ndim") and v.ndim == 0:
            v = v.item()
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            v = None
        out[k] = v
    return out

def _clean_rows(rows): return [_clean(r) for r in rows]

def _parse_raw(record: dict) -> dict:
    try:
        raw = json.loads(record.get("raw_json") or "{}")
    except Exception:
        raw = {}

    for field in (
        "description", "why_failed", "full_article", "outcome",
        "country", "category", "funding_range",

        "employees",

        "num_founders", "num_employees", "num_funding_rounds", "num_investors",

        "_source", "_file", "_lootdrop_id",

        "_detail_market_analysis", "_detail_market_potential_full",
        "_detail_difficulty_full", "_detail_scalability_full",
        "_detail_rebuild_concept",
    ):
        if field not in record or not record[field]:
            record[field] = raw.get(field, "")
    return record

@router.get("/stats")
def failure_stats():
    con = get_db()

    total = int(con.execute("SELECT COUNT(*) FROM failures_unified").fetchone()[0])

    raw_reasons = con.execute("""
        SELECT failure_reasons
        FROM failures_unified
        WHERE failure_reasons IS NOT NULL
    """).fetchdf()

    from collections import Counter
    all_reasons = []
    for row in raw_reasons["failure_reasons"]:
        if isinstance(row, list):
            all_reasons.extend(row)
        elif hasattr(row, "tolist"):
            all_reasons.extend(row.tolist())

    top_reasons = [
        {"reason": r, "count": c}
        for r, c in Counter(all_reasons).most_common(10)
        if r and r != "unknown"
    ]
    by_sector = con.execute("""
        SELECT
            sector,
            COUNT(*)::INTEGER                                        AS count,
            COALESCE(AVG(funding_raised_usd)
                FILTER (WHERE funding_raised_usd > 0), 0)           AS avg_funding
        FROM failures_unified
        WHERE sector IS NOT NULL AND sector != 'Other'
        GROUP BY sector
        ORDER BY count DESC
        LIMIT 15
    """).fetchdf().fillna(0).to_dict(orient="records")

    by_year = con.execute("""
        SELECT
            year_failed,
            COUNT(*)::INTEGER AS count
        FROM failures_unified
        WHERE year_failed IS NOT NULL AND year_failed >= 2000
        GROUP BY year_failed
        ORDER BY year_failed DESC
        LIMIT 20
    """).fetchdf().to_dict(orient="records")

    by_source = con.execute("""
        SELECT
            COALESCE(CAST(raw_json->>'_source' AS VARCHAR), 'unknown') AS source,
            COUNT(*)::INTEGER AS count
        FROM failures_unified
        GROUP BY source
        ORDER BY count DESC
    """).fetchdf().to_dict(orient="records")

    funding_stats = con.execute("""
        SELECT
            COALESCE(AVG(funding_raised_usd)
                FILTER (WHERE funding_raised_usd > 0), 0)    AS avg_burned,
            COALESCE(SUM(funding_raised_usd)
                FILTER (WHERE funding_raised_usd > 0), 0)    AS total_burned,
            COUNT(*) FILTER (WHERE funding_raised_usd > 0)   AS has_funding
        FROM failures_unified
    """).fetchone()

    return {
        "total":                 total,
        "top_reasons":           _clean_rows(top_reasons),
        "by_sector":             _clean_rows(by_sector),
        "by_year":               _clean_rows(by_year),
        "by_source":             _clean_rows(by_source),
        "avg_funding_burned":    round(float(funding_stats[0] or 0), 2),
        "total_funding_burned":  round(float(funding_stats[1] or 0), 2),
        "failures_with_funding": int(funding_stats[2] or 0),
    }

@router.get("")
def list_failures(
    q:        str = Query(None,
                          description="Search company name, description, why_failed, key_lesson, full_article"),
    sector:   str = Query(None,  description="Filter by sector"),
    reason:   str = Query(None,  description="Filter by failure reason tag"),
    min_year: int = Query(None,  description="Failures from this year onwards"),
    max_year: int = Query(None,  description="Failures up to this year"),
    source:   str = Query(None,  description="cbinsights | failory | lootdrop"),
    country:  str = Query(None,  description="Filter by country"),
    limit:    int = Query(20,    ge=1, le=100),
    offset:   int = Query(0,     ge=0),
):
    con    = get_db()
    wheres = []
    params = []

    if q:
        wheres.append("""(
            company_name                                           ILIKE ?
            OR key_lesson                                          ILIKE ?
            OR CAST(raw_json->>'description'  AS VARCHAR)         ILIKE ?
            OR CAST(raw_json->>'why_failed'   AS VARCHAR)         ILIKE ?
            OR CAST(raw_json->>'full_article' AS VARCHAR)         ILIKE ?
        )""")
        params += [f"%{q}%"] * 5

    if sector:
        wheres.append("sector ILIKE ?")
        params.append(f"%{sector}%")

    if reason:
        wheres.append("? = ANY(failure_reasons)")
        params.append(reason)

    if min_year:
        wheres.append("year_failed >= ?")
        params.append(min_year)

    if max_year:
        wheres.append("year_failed <= ?")
        params.append(max_year)

    if source:
        wheres.append("CAST(raw_json->>'_source' AS VARCHAR) = ?")
        params.append(source)

    if country:
        wheres.append("CAST(raw_json->>'country' AS VARCHAR) ILIKE ?")
        params.append(f"%{country}%")

    where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""

    total = int(con.execute(
        f"SELECT COUNT(*) FROM failures_unified {where_clause}", params
    ).fetchone()[0])

    rows = con.execute(f"""
        SELECT
            failure_id,
            company_name,
            sector,
            year_founded,
            year_failed,
            funding_raised_usd,
            failure_reasons,
            key_lesson,
            founder_names,
            source_url,
            CAST(raw_json->>'_source'            AS VARCHAR) AS source,
            CAST(raw_json->>'why_failed'          AS VARCHAR) AS why_failed,
            CAST(raw_json->>'description'         AS VARCHAR) AS description,
            CAST(raw_json->>'outcome'             AS VARCHAR) AS outcome,
            CAST(raw_json->>'country'             AS VARCHAR) AS country,
            CAST(raw_json->>'funding_range'       AS VARCHAR) AS funding_range,
            CAST(raw_json->>'employees'           AS VARCHAR) AS employees,
            CAST(raw_json->>'num_founders'        AS VARCHAR) AS num_founders,
            CAST(raw_json->>'num_investors'       AS VARCHAR) AS num_investors,
            CAST(raw_json->>'num_funding_rounds'  AS VARCHAR) AS num_funding_rounds,
            CAST(raw_json->>'num_employees'       AS VARCHAR) AS num_employees
        FROM failures_unified
        {where_clause}
        ORDER BY year_failed DESC NULLS LAST, funding_raised_usd DESC NULLS LAST
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchdf()

    return {
        "total":   total,
        "limit":   limit,
        "offset":  offset,
        "results": _clean_rows(rows.fillna("").to_dict(orient="records")),
    }

@router.get("/sector/{sector_name}")
def failures_by_sector(sector_name: str):
    con = get_db()

    failures = con.execute("""
        SELECT
            failure_id, company_name, sector,
            year_founded, year_failed, funding_raised_usd,
            failure_reasons, key_lesson, founder_names, source_url,
            CAST(raw_json->>'why_failed'   AS VARCHAR) AS why_failed,
            CAST(raw_json->>'description'  AS VARCHAR) AS description,
            CAST(raw_json->>'outcome'      AS VARCHAR) AS outcome,
            CAST(raw_json->>'country'      AS VARCHAR) AS country,
            CAST(raw_json->>'_source'      AS VARCHAR) AS source
        FROM failures_unified
        WHERE sector ILIKE ?
        ORDER BY funding_raised_usd DESC NULLS LAST
    """, [f"%{sector_name}%"]).fetchdf()

    if failures.empty:
        return {"sector": sector_name, "total": 0, "failures": [],
                "risk_summary": {}}

    all_reasons = [
        r for reasons in failures["failure_reasons"].tolist()
        for r in (reasons if isinstance(reasons, list) else [])
    ]
    top_reasons = Counter(all_reasons).most_common(5)

    funding_vals = failures["funding_raised_usd"].dropna()
    funding_vals = funding_vals[funding_vals > 0]
    avg_funding  = float(funding_vals.mean()) if len(funding_vals) > 0 else 0

    n          = len(failures)
    risk_level = "LOW" if n < 3 else "MEDIUM" if n < 10 else "HIGH"

    years = failures["year_failed"].dropna()
    year_range = (
        {"from": int(years.min()), "to": int(years.max())}
        if len(years) > 0 else {}
    )

    source_col = failures["source"].fillna("unknown").tolist()
    src_counts = dict(Counter(source_col))

    return {
        "sector":   sector_name,
        "total":    n,
        "failures": _clean_rows(failures.fillna("").to_dict(orient="records")),
        "risk_summary": {
            "risk_level":         risk_level,
            "total_failures":     n,
            "top_reasons":        [{"reason": r, "count": c}
                                   for r, c in top_reasons],
            "avg_funding_burned": round(avg_funding, 2),
            "year_range":         year_range,
            "by_source":          src_counts,
        },
    }

@router.get("/{failure_id}")
def get_failure(failure_id: str):
    con = get_db()
    row = con.execute(
        "SELECT * FROM failures_unified WHERE failure_id = ?", [failure_id]
    ).fetchdf()

    if row.empty:
        raise HTTPException(status_code=404,
                            detail=f"Failure '{failure_id}' not found.")

    record = _clean(row.fillna("").to_dict(orient="records")[0])
    record = _parse_raw(record)

    enr = con.execute("""
        SELECT full_text, summary, key_fields, enrichment_status
        FROM enriched_details
        WHERE record_id = ?
        ORDER BY enriched_at DESC
        LIMIT 1
    """, [failure_id]).fetchdf()

    if not enr.empty:
        enr_row = enr.fillna("").to_dict(orient="records")[0]
        record["full_text"]         = enr_row.get("full_text", "")
        record["summary"]           = enr_row.get("summary", "")
        record["enrichment_status"] = enr_row.get("enrichment_status", "")
        try:
            kf = json.loads(enr_row.get("key_fields") or "{}")
            record["key_fields"] = kf

            for promoted in (
                "difficulty", "difficulty_reason",
                "scalability", "scalability_reason",
                "market_potential", "market_potential_reason",
                "market_analysis", "market_potential_full",
                "difficulty_full", "scalability_full",
                "rebuild_concept", "product_type", "views",
            ):
                if promoted not in record or not record[promoted]:
                    record[promoted] = kf.get(promoted, "")
        except Exception:
            record["key_fields"] = {}

    return record

"""
GET /api/failures  
GET /api/failures/stats
GET /api/failures/{id}      
"""