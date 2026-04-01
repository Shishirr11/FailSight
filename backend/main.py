import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from storage.db import get_db
from storage.schema import create_schema

from routers import opportunities
from routers import will_not_use_this_probably
from routers import failures
from routers import search
from routers import watchlist
from routers import briefings
from routers import pipeline       

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting TBD backend...")
    con = get_db()
    create_schema(con)
    logger.success("Database ready.")
    yield
    logger.info("Shutting down TBD backend.")


app = FastAPI(
    title="Founder Opportunity Intelligence Platform",
    description="Aggregates public opportunity signals for founders and students.",
    version="0.1.0",
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
app.include_router(will_not_use_this_probably.router,       prefix="/api/sectors",       tags=["Sectors"])
app.include_router(failures.router,      prefix="/api/failures",      tags=["Failures"])
app.include_router(search.router,        prefix="/api/search",        tags=["Search"])
app.include_router(watchlist.router,     prefix="/api/watchlist",     tags=["Watchlist"])
app.include_router(briefings.router,     prefix="/api/briefings",     tags=["Briefings"])
app.include_router(pipeline.router,      prefix="/api/pipeline",      tags=["Pipeline"])  # ← NEW


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