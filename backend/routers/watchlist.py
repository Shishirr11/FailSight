import math
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from loguru import logger

from storage.db import get_db

router = APIRouter()

class WatchlistCreate(BaseModel):
    user_label:  str
    keyword:     str        = ""
    sectors:     list[str]  = []
    min_funding: float      = 0.0
    sources:     list[str]  = []

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

def _to_list(val) -> list:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x) for x in val if x]
    if hasattr(val, "tolist"):
        return [str(x) for x in val.tolist() if x]
    if isinstance(val, str):
        return [s.strip() for s in val.split(",") if s.strip()]
    return []

def _run_watchlist_query(item: dict, con, days_back: int = 1) -> list[dict]:
    
    wheres = [
        f"posted_date >= CURRENT_DATE - INTERVAL '{days_back} DAYS'"
    ]
    params = []

    keyword = item.get("keyword") or ""
    if keyword:
        wheres.append("(title ILIKE ? OR description ILIKE ?)")
        params += [f"%{keyword}%", f"%{keyword}%"]

    sectors = _to_list(item.get("sectors"))
    if sectors:
        placeholders = ", ".join("?" for _ in sectors)
        wheres.append(f"sector IN ({placeholders})")
        params += sectors

    sources = _to_list(item.get("sources"))
    if sources:
        placeholders = ", ".join("?" for _ in sources)
        wheres.append(f"source IN ({placeholders})")
        params += sources

    min_funding = item.get("min_funding") or 0
    if min_funding > 0:
        wheres.append("funding_max >= ?")
        params.append(min_funding)

    where_clause = "WHERE " + " AND ".join(wheres)

    rows = con.execute(f"""
        SELECT
            opp_id, source, title, sector, agency,
            funding_max,
            posted_date::VARCHAR AS posted_date,
            close_date::VARCHAR  AS close_date
        FROM unified_opportunities
        {where_clause}
        ORDER BY posted_date DESC
        LIMIT 20
    """, params).fetchdf()

    return [_clean(r) for r in rows.fillna("").to_dict(orient="records")]


@router.get("")
def list_watchlist():
    con  = get_db()
    rows = con.execute("""
        SELECT
            id, user_label, keyword, sectors, min_funding,
            sources, created_at::VARCHAR AS created_at,
            last_alerted::VARCHAR AS last_alerted
        FROM watchlist
        ORDER BY created_at DESC
    """).fetchdf()

    if rows.empty:
        return {"items": []}

    return {"items": [_clean(r) for r in rows.fillna("").to_dict(orient="records")]}


@router.post("")
def create_watchlist_item(item: WatchlistCreate):
    con = get_db()

    max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM watchlist").fetchone()[0]
    new_id = int(max_id) + 1

    con.execute("""
        INSERT INTO watchlist (id, user_label, keyword, sectors, min_funding, sources, created_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [
        new_id,
        item.user_label,
        item.keyword,
        item.sectors,
        item.min_funding,
        item.sources,
    ])

    logger.info(f"Watchlist item created: '{item.user_label}' (id={new_id})")
    return {"id": new_id, "message": f"Watchlist item '{item.user_label}' saved."}


@router.delete("/{item_id}")
def delete_watchlist_item(item_id: int):
    con = get_db()

    existing = con.execute(
        "SELECT id FROM watchlist WHERE id = ?", [item_id]
    ).fetchone()

    if not existing:
        raise HTTPException(status_code=404, detail=f"Watchlist item {item_id} not found.")

    con.execute("DELETE FROM watchlist WHERE id = ?", [item_id])
    logger.info(f"Watchlist item {item_id} deleted.")
    return {"message": f"Watchlist item {item_id} deleted."}


@router.post("/{item_id}/check")
def check_watchlist_item(item_id: int, days_back: int = 7):
    con = get_db()

    row = con.execute(
        "SELECT * FROM watchlist WHERE id = ?", [item_id]
    ).fetchdf()

    if row.empty:
        raise HTTPException(status_code=404, detail=f"Watchlist item {item_id} not found.")

    item    = row.to_dict(orient="records")[0]
    matches = _run_watchlist_query(item, con, days_back=days_back)

    con.execute(
        "UPDATE watchlist SET last_alerted = CURRENT_TIMESTAMP WHERE id = ?",
        [item_id]
    )

    return {
        "watchlist_id":  item_id,
        "label":         item.get("user_label", ""),
        "days_back":     days_back,
        "total_matches": len(matches),
        "matches":       matches,
    }

@router.get("/check-all")
def check_all_watchlist(days_back: int = 7):
    con  = get_db()
    rows = con.execute("SELECT * FROM watchlist").fetchdf()

    if rows.empty:
        return {"message": "No watchlist items saved.", "results": []}

    results = []
    for _, item in rows.iterrows():
        item_dict = item.to_dict()
        matches   = _run_watchlist_query(item_dict, con, days_back=days_back)

        results.append({
            "watchlist_id":  int(item_dict.get("id", 0)),
            "label":         item_dict.get("user_label", ""),
            "total_matches": len(matches),
            "matches":       matches[:5],   

        })

        con.execute(
            "UPDATE watchlist SET last_alerted = CURRENT_TIMESTAMP WHERE id = ?",
            [int(item_dict["id"])]
        )

    total_matches = sum(r["total_matches"] for r in results)
    logger.info(f"Watchlist check-all: {total_matches} total matches across {len(results)} items")

    return {
        "checked":       len(results),
        "total_matches": total_matches,
        "results":       results,
    }