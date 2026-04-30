import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from storage.db import init_db, get_db, TMP_INDEX
from storage.schema import create_schema
from routers import groq as groq_router
from routers import opportunities
from routers import failures
from routers import search
from routers import briefings
from routers import pipeline
from routers import storage as storage_router

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Failsight backend…")

    try:
        con = init_db()
        create_schema(con)
        logger.success("Database ready.")
    except RuntimeError as e:
        logger.error(f"DB init failed: {e}")
        app.state.db_unavailable = str(e)
        yield
        return

    app.state.db_unavailable = None

    try:
        import sys
        for candidate in [
            Path(__file__).resolve().parent.parent / "scripts",
            Path(__file__).resolve().parent / "scripts",
            Path("/app/scripts"),
        ]:
            if candidate.exists() and str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
        import query_engine
        query_engine._load_tfidf()
        logger.success("TF-IDF index pre-loaded.")
    except Exception as e:
        logger.warning(f"Search pre-warm failed (non-fatal): {e}")

    yield
    logger.info("Shutting down.")


app = FastAPI(
    title="Failsight — Founder Intelligence Platform",
    version="0.3.0",
    lifespan=lifespan,
)

origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def db_availability_gate(request: Request, call_next):
    unavailable = getattr(app.state, "db_unavailable", None)
    passthrough = (
        request.url.path.startswith("/api/health") or
        request.url.path.startswith("/api/storage") or
        request.url.path == "/"
    )
    if unavailable and not passthrough:
        return JSONResponse(
            status_code=503,
            content={
                "error":   "service_unavailable",
                "message": str(unavailable),
                "hint":    "R2 storage cap may have been reached. Caps reset on the 1st of each month.",
            },
        )
    return await call_next(request)


app.include_router(opportunities.router,  prefix="/api/opportunities", tags=["Opportunities"])
app.include_router(failures.router,       prefix="/api/failures",      tags=["Failures"])
app.include_router(search.router,         prefix="/api/search",        tags=["Search"])
app.include_router(briefings.router,      prefix="/api/briefings",     tags=["Briefings"])
app.include_router(pipeline.router,       prefix="/api/pipeline",      tags=["Pipeline"])
app.include_router(groq_router.router,    prefix="/api/groq",          tags=["Groq AI"])
app.include_router(storage_router.router, prefix="/api/storage",       tags=["Storage"])


@app.get("/api/health", tags=["Health"])
def health():
    unavailable = getattr(app.state, "db_unavailable", None)
    if unavailable:
        return JSONResponse(status_code=503, content={"status": "degraded", "reason": unavailable})

    con = get_db()
    row_count = con.execute("SELECT COUNT(*) FROM unified_opportunities").fetchone()[0]

    r2_status = "not_configured"
    if os.environ.get("R2_ACCOUNT_ID"):
        try:
            from storage.r2_store import get_usage_report
            usage = get_usage_report()
            any_exceeded = (
                usage["storage"]["exceeded"] or
                usage["reads"]["exceeded"]   or
                usage["writes"]["exceeded"]
            )
            r2_status = "exceeded" if any_exceeded else "ok"
        except Exception:
            r2_status = "unreachable"

    return {
        "status":              "ok",
        "opportunities_in_db": row_count,
        "r2_storage":          r2_status,
    }


'''
The Requirement.txt for local hosting
fastapi
uvicorn
requests

# Data
pandas
pyarrow
duckdb

# NLP / Search
spacy
sentence-transformers
scikit-learn

# Collectors
pyalex
httpx
beautifulsoup4

# Infra
apscheduler
python-dotenv
loguru
tqdm
pytest
'''