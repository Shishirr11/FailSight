import json
import math
import sys
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from loguru import logger

from storage.db import get_db

router = APIRouter()


def _get_engine():
    _scripts = Path(__file__).resolve().parent.parent.parent / "scripts"
    if str(_scripts) not in sys.path:
        sys.path.insert(0, str(_scripts))
    try:
        import query_engine
        return query_engine
    except ImportError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Query engine not found at {_scripts}. "
                   f"Make sure scripts/query_engine.py exists. Error: {e}"
        )



class SearchRequest(BaseModel):
    query:            str
    sources:          list[str] = []
    limit:            int       = 20
    offset:           int       = 0
    include_failures: bool      = True
    use_embeddings:   bool      = True


class CompareRequest(BaseModel):
    sectors: list[str] = []



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



@router.post("")
def semantic_search(req: SearchRequest):
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    engine = _get_engine()
    con    = get_db()

    logger.info(f"Search: '{req.query}'  sources={req.sources or 'all'}")

    results = engine.search(
        query            = req.query,
        sources          = req.sources or None,
        limit            = req.limit,
        offset           = req.offset,
        include_failures = req.include_failures,
        use_embeddings   = req.use_embeddings,
        con              = con,
    )

    results["results"]  = [_clean(r) for r in results.get("results", [])]
    results["failures"] = [_clean(f) for f in results.get("failures", [])]

    return results


@router.post("/compare")
def compare_domains(req: CompareRequest):
    engine = _get_engine()
    con    = get_db()
    data   = engine.compare_domains(req.sectors, con=con)
    return {"sectors": data}


@router.get("/suggest")
def suggest(q: str = Query("", min_length=0)) -> dict:
    """Autocomplete sectors and agencies for the search bar."""
    if len(q) < 2:
        return {"sectors": [], "agencies": []}

    con = get_db()

    sectors = con.execute("""
        SELECT DISTINCT sector
        FROM unified_opportunities
        WHERE sector ILIKE ? AND sector IS NOT NULL
        ORDER BY sector LIMIT 6
    """, [f"%{q}%"]).fetchall()

    agencies = con.execute("""
        SELECT DISTINCT agency
        FROM unified_opportunities
        WHERE agency ILIKE ? AND agency IS NOT NULL AND agency != ''
        ORDER BY agency LIMIT 6
    """, [f"%{q}%"]).fetchall()

    return {
        "sectors":  [r[0] for r in sectors],
        "agencies": [r[0] for r in agencies],
    }


@router.get("/health")
def search_health():
    from pathlib import Path
    _backend = Path(__file__).resolve().parent.parent

    index_dir    = _backend / "data" / "search_index"
    tfidf_ready  = (index_dir / "tfidf_matrix.npy").exists()
    embed_ready  = (index_dir / "embeddings_matrix.npy").exists()

    tfidf_size   = None
    embed_size   = None
    tfidf_count  = None
    embed_count  = None

    if tfidf_ready:
        ids_path    = index_dir / "tfidf_record_ids.json"
        tfidf_count = len(json.loads(ids_path.read_text())) if ids_path.exists() else None
        tfidf_size  = round((index_dir / "tfidf_matrix.npy").stat().st_size / 1e6, 1)

    if embed_ready:
        ids_path    = index_dir / "embedding_record_ids.json"
        embed_count = len(json.loads(ids_path.read_text())) if ids_path.exists() else None
        embed_size  = round((index_dir / "embeddings_matrix.npy").stat().st_size / 1e6, 1)

    return {
        "tfidf": {
            "ready":   tfidf_ready,
            "records": tfidf_count,
            "size_mb": tfidf_size,
        },
        "embeddings": {
            "ready":   embed_ready,
            "records": embed_count,
            "size_mb": embed_size,
        },
        "mode": (
            "tfidf+embeddings" if (tfidf_ready and embed_ready) else
            "tfidf"            if tfidf_ready else
            "sql_fallback"
        ),
    }