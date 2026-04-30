import sys
import json
import math
import pickle
import re
import argparse
from pathlib import Path
from typing import Optional

import numpy as np

_HERE    = Path(__file__).resolve()
_BACKEND = _HERE.parent.parent
sys.path.insert(0, str(_BACKEND))

from storage.db import get_db
from loguru import logger

import os as _os
_TMP_INDEX  = Path(_os.environ.get("ASSET_CACHE_DIR", "/tmp/failsight")) / "index"
_LOCAL_INDEX = _BACKEND / "data" / "search_index"

INDEX_DIR = _TMP_INDEX if (_TMP_INDEX / "tfidf_matrix.npz").exists() else _LOCAL_INDEX

TFIDF_VEC_PATH  = INDEX_DIR / "tfidf_vectorizer.pkl"
TFIDF_MAT_PATH  = INDEX_DIR / "tfidf_matrix.npz"
TFIDF_IDS_PATH  = INDEX_DIR / "tfidf_record_ids.json"
EMBED_MAT_PATH  = INDEX_DIR / "embeddings_matrix.npy"
EMBED_IDS_PATH  = INDEX_DIR / "embedding_record_ids.json"


SECTOR_KEYWORDS: dict[str, list[str]] = {
    "AI & Machine Learning":    ["artificial intelligence", "machine learning", "deep learning", "neural", "llm", "nlp"],
    "Cybersecurity":            ["cybersecurity", "zero trust", "threat", "vulnerability", "encryption"],
    "Clean Energy":             ["renewable energy", "solar", "wind", "energy storage", "battery", "grid"],
    "Climate Technology":       ["climate", "carbon", "emissions", "sustainability", "carbon capture"],
    "Biotechnology":            ["biotech", "genomics", "crispr", "protein", "synthetic biology"],
    "Health Technology":        ["health", "medical", "telehealth", "clinical", "diagnostic", "therapeutics"],
    "Quantum Computing":        ["quantum", "qubit", "quantum computing", "quantum sensing"],
    "Advanced Manufacturing":   ["manufacturing", "additive", "3d printing", "robotics", "automation"],
    "Aerospace & Defense":      ["aerospace", "defense", "drone", "satellite", "hypersonic", "propulsion"],
    "Agriculture Technology":   ["agtech", "precision agriculture", "food safety", "crop", "soil"],
    "Advanced Computing":       ["semiconductor", "chip", "microelectronics", "hpc", "edge computing"],
}

STOPWORDS = {
    "the", "a", "an", "and", "or", "for", "to", "in", "of", "with",
    "is", "was", "are", "be", "been", "that", "this", "on", "at", "by",
    "from", "as", "it", "its", "we", "our", "i", "my", "me", "us",
    "have", "has", "had", "will", "would", "can", "could", "should",
    "do", "does", "did", "not", "but", "so", "if", "about", "into",
    "new", "using", "based", "system", "project", "program",
}


def _clean_val(v):
    if isinstance(v, np.ndarray):
        return v.tolist()
    if hasattr(v, "ndim") and v.ndim == 0 and hasattr(v, "item"):
        v = v.item()
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _clean_row(r: dict) -> dict:
    return {k: _clean_val(v) for k, v in r.items()}

def interpret_query(query: str) -> dict:
    q_lower = query.lower()
    tokens  = re.findall(r'\b[a-z][a-z0-9\-]{2,}\b', q_lower)

    keywords = [t for t in tokens if t not in STOPWORDS]
    seen = set()
    keywords = [k for k in keywords if not (k in seen or seen.add(k))]

    sectors = []
    for sector, kws in SECTOR_KEYWORDS.items():
        if any(kw in q_lower for kw in kws):
            sectors.append(sector)

    min_funding = None
    max_funding = None
    fund_match = re.findall(r'\$\s*(\d+(?:\.\d+)?)\s*([kmb]?)', q_lower)
    for amount, suffix in fund_match:
        val = float(amount)
        if suffix == 'k':   val *= 1_000
        elif suffix == 'm': val *= 1_000_000
        elif suffix == 'b': val *= 1_000_000_000
        if min_funding is None:
            min_funding = val
        else:
            max_funding = val

    return {
        "keywords":    keywords[:8],
        "sectors":     sectors,
        "intent":      query,
        "min_funding": min_funding,
        "max_funding": max_funding,
    }


_tfidf_cache: dict = {}

def _load_tfidf():
    if _tfidf_cache:
        return _tfidf_cache
    if not TFIDF_VEC_PATH.exists():
        return {}
    logger.debug("Loading TF-IDF index from disk...")
    with open(TFIDF_VEC_PATH, "rb") as f:
        vectorizer = pickle.load(f)
    from scipy.sparse import load_npz
    matrix = load_npz(str(TFIDF_MAT_PATH))
    rec_ids = json.loads(TFIDF_IDS_PATH.read_text())
    _tfidf_cache.update({"vectorizer": vectorizer, "matrix": matrix, "ids": rec_ids})
    logger.debug(f"TF-IDF index loaded — {len(rec_ids):,} records, {matrix.shape[1]:,} features")
    return _tfidf_cache


def tfidf_scores(query: str, top_n: int = 500) -> list[tuple[str, float]]:
    idx = _load_tfidf()
    if not idx:
        return []
    vec    = idx["vectorizer"]
    matrix = idx["matrix"]
    ids    = idx["ids"]
    q_vec  = vec.transform([query])  # keep sparse
    if q_vec.nnz == 0:
        return []
    scores = (matrix @ q_vec.T).toarray().flatten()
    top    = np.argsort(scores)[::-1][:top_n]
    return [(ids[i], float(scores[i])) for i in top if scores[i] > 0]


_embed_cache: dict = {}

def _load_embeddings():
    if _embed_cache:
        return _embed_cache
    if not EMBED_MAT_PATH.exists():
        return {}
    logger.debug("Loading embedding index from disk...")
    matrix  = np.load(EMBED_MAT_PATH)
    rec_ids = json.loads(EMBED_IDS_PATH.read_text())
    _embed_cache.update({"matrix": matrix, "ids": rec_ids})
    logger.debug(f"Embeddings loaded — {len(rec_ids):,} vectors, dim={matrix.shape[1]}")
    return _embed_cache


def embed_query(query: str) -> Optional[np.ndarray]:
    try:
        from sentence_transformers import SentenceTransformer
        import torch
        if not hasattr(embed_query, "_model"):
            device = "mps"  if torch.backends.mps.is_available()  else \
                     "cuda" if torch.cuda.is_available()           else "cpu"
            embed_query._model = SentenceTransformer("all-mpnet-base-v2", device=device)
            logger.debug(f"Embedding model loaded on {device}")
        vec = embed_query._model.encode([query], normalize_embeddings=True)[0]
        return vec.astype(np.float32)
    except Exception as e:
        logger.warning(f"Embedding failed: {e}")
        return None


def embedding_scores(query: str, top_n: int = 500) -> list[tuple[str, float]]:
    idx = _load_embeddings()
    if not idx:
        return []
    q_vec = embed_query(query)
    if q_vec is None:
        return []
    matrix = idx["matrix"]
    ids    = idx["ids"]
    scores = matrix @ q_vec
    top    = np.argsort(scores)[::-1][:top_n]
    return [(ids[i], float(scores[i])) for i in top if scores[i] > 0.15]


def fuse_scores(
    tfidf_results: list[tuple[str, float]],
    embed_results: list[tuple[str, float]],
    tfidf_weight: float = 0.5,
    embed_weight: float = 0.5,
    rrf_k: int = 60,
) -> dict[str, float]:
    fused: dict[str, float] = {}
    for rank, (rid, _) in enumerate(tfidf_results):
        fused[rid] = fused.get(rid, 0.0) + tfidf_weight / (rrf_k + rank + 1)
    for rank, (rid, _) in enumerate(embed_results):
        fused[rid] = fused.get(rid, 0.0) + embed_weight / (rrf_k + rank + 1)
    return fused


def explain_match(query_keywords: list[str], record: dict) -> str:
    title   = (record.get("title")       or "").lower()
    desc    = (record.get("description") or "").lower()
    sector  = (record.get("sector")      or "")
    source  = (record.get("source")      or "").upper()
    agency  = (record.get("agency")      or "")
    matched_kws = [kw for kw in query_keywords if kw in title or kw in desc]
    parts = []
    if matched_kws:
        parts.append(f"Matches on: {', '.join(matched_kws[:3])}")
    if sector:
        parts.append(f"sector={sector}")
    if agency:
        parts.append(f"via {agency[:40]}")
    return " | ".join(parts) if parts else f"{source} record relevant to query"


def search_failures(keywords: list[str], sectors: list[str],
                    limit: int = 10, con=None) -> list[dict]:
    if con is None:
        con = get_db()

    wheres = []
    params = []

    if keywords:
        kw_clauses = []
        for kw in keywords[:5]:
            kw_clauses.append(
                "(company_name ILIKE ? OR sector ILIKE ? OR key_lesson ILIKE ?)"
            )
            params += [f"%{kw}%", f"%{kw}%", f"%{kw}%"]
        wheres.append("(" + " OR ".join(kw_clauses) + ")")

    if sectors:
        sector_or = " OR ".join("sector ILIKE ?" for _ in sectors)
        wheres.append(f"({sector_or})")
        params += [f"%{s}%" for s in sectors]

    where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""

    rows = con.execute(f"""
        SELECT
            failure_id, company_name, sector,
            year_founded, year_failed, funding_raised_usd,
            failure_reasons, key_lesson, stage_at_failure
        FROM failures_unified
        {where_sql}
        ORDER BY funding_raised_usd DESC NULLS LAST
        LIMIT ?
    """, params + [limit]).fetchdf()

    return [_clean_row(r) for r in rows.to_dict(orient="records")]


def compare_domains(sectors: list[str], con=None) -> list[dict]:
    if con is None:
        con = get_db()

    if not sectors:
        rows = con.execute("""
            SELECT sector, COUNT(*) as cnt
            FROM unified_opportunities
            WHERE sector IS NOT NULL
            GROUP BY sector ORDER BY cnt DESC LIMIT 6
        """).fetchall()
        sectors = [r[0] for r in rows]

    results = []
    max_opps = 1

    for sector in sectors:
        opps = con.execute("""
            SELECT COUNT(*) FROM unified_opportunities
            WHERE sector ILIKE ?
              AND (close_date IS NULL OR close_date >= CURRENT_DATE)
        """, [f"%{sector}%"]).fetchone()[0]

        avg_fund = con.execute("""
            SELECT COALESCE(AVG(funding_max), 0)
            FROM unified_opportunities
            WHERE sector ILIKE ?
              AND funding_max IS NOT NULL AND funding_max > 0
        """, [f"%{sector}%"]).fetchone()[0] or 0

        failures = con.execute("""
            SELECT COUNT(*) FROM failures_unified
            WHERE sector ILIKE ?
        """, [f"%{sector}%"]).fetchone()[0]

        opps_int = int(opps)
        max_opps = max(max_opps, opps_int)

        results.append({
            "sector":            sector,
            "opportunity_count": opps_int,
            "avg_funding":       round(float(avg_fund), 2),
            "failure_count":     int(failures),
            "failure_rate":      round(int(failures) / max(opps_int, 1), 3),
        })

    for r in results:
        r["match_pct"] = round(r["opportunity_count"] / max_opps * 100, 1)

    results.sort(key=lambda x: x["opportunity_count"], reverse=True)
    return results


def search(
    query:            str,
    sources:          list[str] = None,
    limit:            int       = 20,
    offset:           int       = 0,
    include_failures: bool      = True,
    use_embeddings:   bool      = False, # True for local hosting, downgraded due to fre hosting limitations
    con=None,
) -> dict:

    if con is None:
        con = get_db()

    interpreted = interpret_query(query)
    keywords    = interpreted["keywords"]
    sectors     = interpreted["sectors"]

    logger.info(f"Query: '{query}'")
    logger.info(f"Keywords: {keywords}")
    logger.info(f"Sectors:  {sectors}")

    tfidf_res = tfidf_scores(query, top_n=300)
    logger.debug(f"TF-IDF candidates: {len(tfidf_res)}")

    embed_res = []
    if use_embeddings and EMBED_MAT_PATH.exists():
        embed_res = embedding_scores(query, top_n=300)
        logger.debug(f"Embedding candidates: {len(embed_res)}")

    if tfidf_res and embed_res:
        fused = fuse_scores(tfidf_res, embed_res)
    elif tfidf_res:
        fused = {rid: score for rid, score in tfidf_res}
    else:
        fused = {}
    if fused:
        ranked_ids = sorted(fused.keys(), key=lambda r: fused[r], reverse=True)

        if sources:
            source_set   = set(sources)
            candidates   = ranked_ids[:600]
            placeholders = ",".join("?" for _ in candidates)
            source_map   = dict(con.execute(f"""
                SELECT opp_id, source FROM unified_opportunities
                WHERE opp_id IN ({placeholders})
            """, candidates).fetchall())
            ranked_ids = [rid for rid in ranked_ids
                          if source_map.get(rid) in source_set]

        max_score = max(fused.values()) if fused else 1.0
        total     = len(ranked_ids)
        batch     = ranked_ids[offset: offset + limit + 10]
        rows      = []
        seen      = set()

        for rid in batch:
            if len(rows) >= limit:
                break
            if rid in seen:
                continue
            seen.add(rid)

            row = con.execute("""
                SELECT
                    u.opp_id, u.source, u.title, u.description,
                    u.sector, u.agency, u.funding_min, u.funding_max,
                    u.posted_date::VARCHAR  AS posted_date,
                    u.close_date::VARCHAR   AS close_date,
                    u.eligibility,
                    array_to_string(u.tags, ',') AS tags
                FROM unified_opportunities u
                JOIN enriched_details e ON e.record_id = u.opp_id
                WHERE e.record_id = ?
            """, [rid]).fetchdf()

            if row.empty:
                continue

            rec           = _clean_row(row.fillna("").to_dict(orient="records")[0])
            raw_score     = fused.get(rid, 0.0)
            relevance_pct = round(raw_score / max_score * 100, 1)
            rec["relevance_pct"] = relevance_pct
            rec["match_reason"]  = explain_match(keywords, rec)
            rows.append(rec)
    else:
        logger.warning("No index found — falling back to SQL keyword search")
        wheres = ["1=1"]
        params: list = []

        if keywords:
            kw_parts = []
            for kw in keywords[:5]:
                kw_parts.append("(title ILIKE ? OR description ILIKE ?)")
                params += [f"%{kw}%", f"%{kw}%"]
            wheres.append("(" + " OR ".join(kw_parts) + ")")

        if sources:
            placeholders = ", ".join("?" for _ in sources)
            wheres.append(f"source IN ({placeholders})")
            params += sources

        wheres.append("(close_date IS NULL OR close_date >= CURRENT_DATE)")
        where_sql = "WHERE " + " AND ".join(wheres)

        total = int(con.execute(
            f"SELECT COUNT(*) FROM unified_opportunities {where_sql}", params
        ).fetchone()[0])

        raw_rows = con.execute(f"""
            SELECT
                opp_id, source, title, description, sector, agency,
                funding_min, funding_max,
                posted_date::VARCHAR AS posted_date,
                close_date::VARCHAR  AS close_date,
                eligibility,
                array_to_string(tags, ',') AS tags
            FROM unified_opportunities
            {where_sql}
            ORDER BY posted_date DESC NULLS LAST
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchdf()

        rows = []
        for rec in raw_rows.fillna("").to_dict(orient="records"):
            rec = _clean_row(rec)
            rec["relevance_pct"] = None
            rec["match_reason"]  = explain_match(keywords, rec)
            rows.append(rec)

    failures = []
    if include_failures:
        failures = search_failures(keywords, sectors, limit=8, con=con)

    domain_comparison = compare_domains(sectors, con=con)

    return {
        "query":             query,
        "interpreted_as":    interpreted,
        "total":             total,
        "results":           rows,
        "failures":          failures,
        "domain_comparison": domain_comparison,
        "index_used": (
            "tfidf+embeddings" if (tfidf_res and embed_res) else
            "tfidf"            if tfidf_res else
            "sql_fallback"
        ),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local query engine — terminal test")
    parser.add_argument("query",           help="Your idea or search query")
    parser.add_argument("--limit",  "-n",  type=int, default=10)
    parser.add_argument("--sources",       nargs="*", default=None)
    parser.add_argument("--no-embed",      action="store_true")
    parser.add_argument("--failures-only", action="store_true")
    args = parser.parse_args()

    results = search(
        query            = args.query,
        sources          = args.sources,
        limit            = args.limit,
        include_failures = True,
        use_embeddings   = not args.no_embed,
    )

    interp = results["interpreted_as"]
    print(f"\n{'═'*65}")
    print(f"  QUERY   : {results['query']}")
    print(f"  INDEX   : {results['index_used']}")
    print(f"  Keywords: {interp['keywords']}")
    print(f"  Sectors : {interp['sectors']}")
    print(f"  Total   : {results['total']} opportunities matched")
    print(f"{'═'*65}")

    print(f"\n── OPPORTUNITIES ({len(results['results'])}) ")
    for i, r in enumerate(results["results"], 1):
        rel = f"{r['relevance_pct']}%" if r["relevance_pct"] is not None else "n/a"
        fund = r.get("funding_max")
        fund_str = f"${fund:,.0f}" if fund else "—"
        print(f"\n  [{i}] {rel:>5}  [{r['source'].upper()}] {r['title'][:70]}")
        print(f"        {r['match_reason']}")
        print(f"        Funding: {fund_str}  |  Closes: {r.get('close_date') or 'open'}")

    if results["failures"]:
        print(f"\n── FAILED STARTUPS ({len(results['failures'])}) ")
        for f in results["failures"]:
            fund = f.get("funding_raised_usd")
            fund_str = f"${fund:,.0f}" if fund else "—"
            print(f"  ✗ {f['company_name']:<30} {fund_str:>10}  {f.get('sector','')}")
            if f.get("key_lesson"):
                print(f"    💡 {str(f['key_lesson'])[:80]}")

    if results["domain_comparison"]:
        print(f"\n── DOMAIN COMPARISON ")
        print(f"  {'Sector':<35} {'Match%':>6} {'Opps':>6} {'AvgFund':>12} {'Failures':>8} {'FailRate':>8}")
        print(f"  {'─'*35} {'─'*6} {'─'*6} {'─'*12} {'─'*8} {'─'*8}")
        for d in results["domain_comparison"]:
            avg = d["avg_funding"]
            avg_str = f"${avg/1e6:.1f}M" if avg >= 1e6 else f"${avg/1e3:.0f}K" if avg >= 1e3 else f"${avg:.0f}"
            print(f"  {d['sector']:<35} {d['match_pct']:>5}% {d['opportunity_count']:>6} "
                  f"{avg_str:>12} {d['failure_count']:>8} {d['failure_rate']:>8.2f}")
    print()