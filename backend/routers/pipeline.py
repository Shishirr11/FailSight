import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from loguru import logger

from storage.db import get_db

router = APIRouter()

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
_INGEST  = _SCRIPTS / "ingest.py"
_TFIDF   = _SCRIPTS / "build_tfidf.py"
_EMBED   = _SCRIPTS / "build_embeddings.py"

_running: dict = {"active": False, "started_at": None, "source": None}


class RunRequest(BaseModel):
    source:        Optional[str] = None  
    rebuild_index: bool          = True  


def _run_pipeline(source: Optional[str], rebuild_index: bool):
    global _running
    _running = {"active": True, "started_at": datetime.now().isoformat(), "source": source or "all"}

    try:
        import sys
        _scripts = Path(__file__).resolve().parent.parent / "scripts"
        if str(_scripts) not in sys.path:
            sys.path.insert(0, str(_scripts))

        from ingest import run_source
        from storage.db import get_db
        from storage.schema import create_schema

        con = get_db()
        create_schema(con)

        sources = [source] if source else ["grants", "research"]
        for src in sources:
            logger.info(f"Pipeline: ingesting {src}...")
            n = run_source(src, con, from_disk=False, fetch_detail=False)
            logger.info(f"Pipeline: {src} +{n} rows")

        if rebuild_index:
            logger.info("Pipeline: rebuilding TF-IDF index...")
            try:
                import build_tfidf
                build_tfidf.build()
                logger.success("TF-IDF rebuilt.")
            except Exception as e:
                logger.error(f"TF-IDF failed: {e}")

            logger.info("Pipeline: rebuilding embeddings...")
            try:
                import build_embeddings
                build_embeddings.build()
                logger.success("Embeddings rebuilt.")
            except Exception as e:
                logger.error(f"Embeddings failed: {e}")

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
    finally:
        _running["active"] = False


@router.post("/run")
def trigger_run(req: RunRequest, background_tasks: BackgroundTasks):
    valid = {"grants", "sam", "research", "patents"}
    if req.source and req.source not in valid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid source '{req.source}'. Choose from: {valid}"
        )

    if _running["active"]:
        return {
            "status":  "already_running",
            "source":  _running["source"],
            "started": _running["started_at"],
            "message": "A pipeline run is already in progress.",
        }

    background_tasks.add_task(_run_pipeline, req.source, req.rebuild_index)

    return {
        "status":  "started",
        "source":  req.source or "all",
        "rebuild_index": req.rebuild_index,
        "message": "Pipeline running in background. Check /api/pipeline/status for updates.",
    }


@router.get("/status")
def pipeline_status():
    con = get_db()

    total_opps     = con.execute("SELECT COUNT(*) FROM unified_opportunities").fetchone()[0]
    total_failures = con.execute("SELECT COUNT(*) FROM failures_unified").fetchone()[0]
    total_enriched = con.execute("SELECT COUNT(*) FROM enriched_details").fetchone()[0]

    by_source = con.execute("""
        SELECT source, COUNT(*) AS count
        FROM unified_opportunities
        GROUP BY source ORDER BY count DESC
    """).fetchdf().fillna(0).to_dict(orient="records")

    funding = con.execute("""
        SELECT
            MIN(funding_max) FILTER (WHERE funding_max > 0),
            MAX(funding_max) FILTER (WHERE funding_max > 0),
            AVG(funding_max) FILTER (WHERE funding_max > 0)
        FROM unified_opportunities
    """).fetchone()

    recent_runs = con.execute("""
        SELECT source, rows_added, status, started_at::VARCHAR AS started_at
        FROM pipeline_log
        ORDER BY started_at DESC
        LIMIT 10
    """).fetchdf().fillna("").to_dict(orient="records")

    last_run = recent_runs[0]["started_at"] if recent_runs else None
    index_dir   = Path(__file__).resolve().parent.parent / "data" / "search_index"
    tfidf_ready = (index_dir / "tfidf_matrix.npz").exists()
    embed_ready = (index_dir / "embeddings_matrix.npy").exists()

    return {
        "running":             _running["active"],
        "last_run":            last_run,
        "total_opportunities": int(total_opps),
        "total_failures":      int(total_failures),
        "total_enriched":      int(total_enriched),
        "by_source":           by_source,
        "funding_range": {
            "min": float(funding[0]) if funding[0] else None,
            "max": float(funding[1]) if funding[1] else None,
            "avg": float(funding[2]) if funding[2] else None,
        },
        "recent_runs": recent_runs,
        "index": {
            "tfidf_ready": tfidf_ready,
            "embed_ready": embed_ready,
            "mode": (
                "tfidf+embeddings" if (tfidf_ready and embed_ready) else
                "tfidf"            if tfidf_ready else
                "sql_fallback"
            ),
        },
    }