import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from storage.db import get_db
from storage.schema import create_schema
from routers import groq as groq_router

from routers import opportunities
from routers import failures
from routers import search
from routers import briefings
from routers import pipeline        

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Findout backend...")
    con = get_db()
    create_schema(con)
    logger.success("Database ready.")
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
    title="Findout — Founder Intelligence Platform",
    version="0.2.0",
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

app.include_router(opportunities.router, prefix="/api/opportunities", tags=["Opportunities"])
app.include_router(failures.router,      prefix="/api/failures",      tags=["Failures"])
app.include_router(search.router,        prefix="/api/search",        tags=["Search"])
app.include_router(briefings.router,     prefix="/api/briefings",     tags=["Briefings"])
app.include_router(pipeline.router,      prefix="/api/pipeline",      tags=["Pipeline"])
app.include_router(groq_router.router, prefix="/api/groq", tags=["Groq AI"])


@app.get("/api/health", tags=["Health"])
def health():
    con = get_db()
    row_count = con.execute(
        "SELECT COUNT(*) FROM unified_opportunities"
    ).fetchone()[0]
    return {
        "status": "ok",
        "opportunities_in_db": row_count,
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