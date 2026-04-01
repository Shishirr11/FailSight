import sys
import json
import uuid
import subprocess
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from loguru import logger
from typing import Optional

from storage.db import get_db

router = APIRouter()


class RunRequest(BaseModel):
    source: Optional[str] = None


def _run_pipeline(source: Optional[str] = None):
    script = Path(__file__).resolve().parent.parent.parent / "scripts" / "run_pipeline.py"
    cmd = [sys.executable, str(script), "--skip-failures"]
    if source:
        cmd += ["--source", source]

    logger.info(f"Pipeline triggered via API: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"Pipeline stderr: {result.stderr[:500]}")
        else:
            logger.success("Pipeline completed via API trigger.")
    except subprocess.TimeoutExpired:
        logger.error("Pipeline timed out after 5 minutes.")
    except Exception as e:
        logger.error(f"Pipeline subprocess error: {e}")


@router.post("/run")
def trigger_run(req: RunRequest, background_tasks: BackgroundTasks):
    valid_sources = {"grants", "sam", "research", "patents"}
    if req.source and req.source not in valid_sources:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid source '{req.source}'. Choose from: {valid_sources}"
        )

    background_tasks.add_task(_run_pipeline, req.source)

    return {
        "status":  "started",
        "source":  req.source or "all",
        "message": "Pipeline is running in the background. Refresh status in ~30 seconds.",
    }


@router.get("/status")
def pipeline_status():
    con = get_db()

    total_opps = con.execute(
        "SELECT COUNT(*) FROM unified_opportunities"
    ).fetchone()[0]

    total_failures = con.execute(
        "SELECT COUNT(*) FROM failures_unified"
    ).fetchone()[0]

    by_source = con.execute("""
        SELECT source, COUNT(*) AS count
        FROM unified_opportunities
        GROUP BY source ORDER BY count DESC
    """).fetchdf().to_dict(orient="records")

    funding = con.execute("""
        SELECT
            MIN(funding_max) FILTER (WHERE funding_max > 0) AS min_funding,
            MAX(funding_max) FILTER (WHERE funding_max > 0) AS max_funding,
            AVG(funding_max) FILTER (WHERE funding_max > 0) AS avg_funding
        FROM unified_opportunities
    """).fetchone()

    recent_runs = con.execute("""
        SELECT source, rows_added, status, started_at::VARCHAR AS started_at
        FROM pipeline_log
        ORDER BY started_at DESC
        LIMIT 10
    """).fetchdf().fillna("").to_dict(orient="records")

    last_run = recent_runs[0]["started_at"] if recent_runs else None

    return {
        "last_run":         last_run,
        "total_opportunities": int(total_opps),
        "total_failures":   int(total_failures),
        "by_source":        by_source,
        "funding_range": {
            "min": float(funding[0]) if funding[0] else None,
            "max": float(funding[1]) if funding[1] else None,
            "avg": float(funding[2]) if funding[2] else None,
        },
        "recent_runs": recent_runs,
    }
"""
POST /api/pipeline/run    — trigger a live data refresh
GET  /api/pipeline/status — last run info + current counts
"""