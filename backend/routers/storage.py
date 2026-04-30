"""
Pipeline router — triggers ingest, rebuilds indexes, exports parquet, uploads to R2
"""
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from loguru import logger

from storage.db import get_db, export_parquet, reload_db_from_parquet, TMP_PARQUET, TMP_INDEX

router = APIRouter()

_SCRIPTS = Path(__file__).resolve().parent.parent.parent / "scripts"

_R2_ENABLED = bool(
    os.environ.get("R2_ACCOUNT_ID") and
    os.environ.get("R2_ACCESS_KEY_ID") and
    os.environ.get("R2_SECRET_ACCESS_KEY")
)

_running: dict = {"active": False, "started_at": None, "source": None, "stage": None}


class RunRequest(BaseModel):
    source:        Optional[str] = None
    rebuild_index: bool          = True


def _run_pipeline(source: Optional[str], rebuild_index: bool):
    global _running
    _running = {
        "active":     True,
        "started_at": datetime.now().isoformat(),
        "source":     source or "all",
        "stage":      "ingest",
    }

    try:
        if str(_SCRIPTS) not in sys.path:
            sys.path.insert(0, str(_SCRIPTS))

        from ingest import run_source
        from storage.schema import create_schema

        con = get_db()
        create_schema(con)

        sources = [source] if source else ["grants", "research"]
        for src in sources:
            logger.info(f"Pipeline: ingesting {src}…")
            n = run_source(src, con, from_disk=False, fetch_detail=False)
            logger.info(f"Pipeline: {src} +{n} rows")

        if rebuild_index:
            _running["stage"] = "tfidf"
            logger.info("Pipeline: rebuilding TF-IDF…")
            try:
                import build_tfidf
                build_tfidf.build()
                logger.success("TF-IDF rebuilt.")
            except Exception as e:
                logger.error(f"TF-IDF rebuild failed: {e}")

        _running["stage"] = "export"
        TMP_PARQUET.mkdir(parents=True, exist_ok=True)
        export_parquet(con, TMP_PARQUET)

        if _R2_ENABLED:
            _running["stage"] = "upload"
            try:
                from storage.r2_store import upload_data_assets, R2CapExceeded
                INDEX_DIR = Path(__file__).resolve().parent.parent / "data" / "search_index"
                result = upload_data_assets(TMP_PARQUET, INDEX_DIR)
                logger.success(f"R2 upload: {len(result['uploaded'])} files.")
            except R2CapExceeded as e:
                logger.error(f"R2 cap exceeded — data NOT uploaded: {e}")
            except Exception as e:
                logger.warning(f"R2 upload failed (non-fatal): {e}")

        _running["stage"] = "reload"
        reload_db_from_parquet(TMP_PARQUET)
        logger.success("In-memory DB reloaded with fresh data.")

    except Exception as e:
        logger.error(f"Pipeline error at stage '{_running.get('stage')}': {e}")
        import traceback; traceback.print_exc()
    finally:
        _running["active"] = False
        _running["stage"]  = None


@router.post("/run")
def trigger_run(req: RunRequest, background_tasks: BackgroundTasks):
    valid = {"grants", "sam", "research", "patents"}
    if req.source and req.source not in valid:
        raise HTTPException(status_code=400, detail=f"Invalid source '{req.source}'. Choose from: {valid}")

    if _running["active"]:
        return {
            "status":  "already_running",
            "source":  _running["source"],
            "started": _running["started_at"],
            "stage":   _running["stage"],
            "message": "A pipeline run is already in progress.",
        }

    background_tasks.add_task(_run_pipeline, req.source, req.rebuild_index)
    return {
        "status":        "started",
        "source":        req.source or "all",
        "rebuild_index": req.rebuild_index,
        "r2_upload":     _R2_ENABLED,
        "message":       "Pipeline running in background.",
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

    INDEX_DIR   = Path(__file__).resolve().parent.parent / "data" / "search_index"
    tfidf_ready = (
        (INDEX_DIR  / "tfidf_matrix.npz").exists() or
        (TMP_INDEX  / "tfidf_matrix.npz").exists()
    )
    embed_ready = (
        (INDEX_DIR  / "embeddings_matrix.npy").exists() or
        (TMP_INDEX  / "embeddings_matrix.npy").exists()
    )

    r2_info = {"enabled": _R2_ENABLED}
    if _R2_ENABLED:
        try:
            from storage.r2_store import get_usage_report
            usage = get_usage_report()
            r2_info.update({
                "storage_pct": usage["storage"]["pct"],
                "reads_pct":   usage["reads"]["pct"],
                "writes_pct":  usage["writes"]["pct"],
                "status": (
                    "exceeded" if (
                        usage["storage"]["exceeded"] or
                        usage["reads"]["exceeded"]   or
                        usage["writes"]["exceeded"]
                    ) else "ok"
                ),
            })
        except Exception as e:
            r2_info["error"] = str(e)

    return {
        "running":             _running["active"],
        "stage":               _running.get("stage"),
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
        "r2": r2_info,
    }