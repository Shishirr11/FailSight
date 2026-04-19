import json
import os
import time
import argparse
from datetime import date
from pathlib import Path
from typing import Optional

import httpx
import pyalex
from pyalex import Works
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

RAW_DIR       = Path(__file__).resolve().parent.parent / "data" / "raw" / "research"
MAX_PER_TOPIC = 140    
MIN_YEAR      = 2021  

PUBMED_SEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_FETCH_URL  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
PUBMED_DELAY      = 0.35   

PUBMED_DELAY_KEY  = 0.11   

RESEARCH_TOPICS: list[tuple[str, str]] = [

    ("large language model transformer",             "AI & Machine Learning"),
    ("generative AI foundation model",               "AI & Machine Learning"),
    ("artificial intelligence deep learning",        "AI & Machine Learning"),
    ("computer vision object detection",             "AI & Machine Learning"),
    ("reinforcement learning autonomous agent",      "AI & Machine Learning"),
    ("natural language processing NLP",              "AI & Machine Learning"),
    ("federated learning privacy preserving",        "AI & Machine Learning"),
    ("explainable AI interpretable model",           "AI & Machine Learning"),

    ("cybersecurity network intrusion detection",    "Cybersecurity"),
    ("zero trust architecture security",             "Cybersecurity"),
    ("adversarial machine learning security",        "Cybersecurity"),
    ("post-quantum cryptography encryption",         "Cybersecurity"),

    ("solar photovoltaic energy efficiency",         "Clean Energy"),
    ("battery energy storage lithium ion",           "Clean Energy"),
    ("hydrogen fuel cell renewable energy",          "Clean Energy"),
    ("grid modernization smart grid electricity",    "Clean Energy"),
    ("nuclear fusion reactor plasma",                "Clean Energy"),
    ("geothermal energy extraction",                 "Clean Energy"),

    ("carbon capture utilization storage",           "Climate Technology"),
    ("climate change adaptation mitigation",         "Climate Technology"),
    ("carbon sequestration soil forest",             "Climate Technology"),

    ("CRISPR gene editing genomics",                 "Biotechnology"),
    ("drug discovery protein structure prediction",  "Biotechnology"),
    ("synthetic biology metabolic engineering",      "Biotechnology"),
    ("mRNA vaccine immunotherapy cancer",            "Biotechnology"),
    ("cell therapy CAR-T immunology",                "Biotechnology"),
    ("microbiome gut bacteria health",               "Biotechnology"),

    ("digital health remote patient monitoring",     "Health Technology"),
    ("precision medicine personalized treatment",    "Health Technology"),
    ("medical imaging AI diagnosis radiology",       "Health Technology"),
    ("mental health technology intervention app",    "Health Technology"),
    ("wearable biosensor health monitoring device",  "Health Technology"),
    ("pandemic preparedness infectious disease",     "Health Technology"),

    ("quantum computing error correction qubit",     "Quantum Computing"),
    ("quantum cryptography communication network",   "Quantum Computing"),
    ("quantum sensing metrology measurement",        "Quantum Computing"),

    ("additive manufacturing 3D printing materials", "Advanced Manufacturing"),
    ("robotics manipulation human robot interaction","Advanced Manufacturing"),
    ("autonomous vehicle self-driving perception",   "Advanced Manufacturing"),
    ("supply chain optimization logistics",          "Advanced Manufacturing"),
    ("advanced materials composite metamaterial",    "Advanced Manufacturing"),

    ("semiconductor chip design fabrication",        "Advanced Computing"),
    ("edge computing IoT distributed systems",       "Advanced Computing"),
    ("photonics integrated circuit optical",         "Advanced Computing"),
    ("neuromorphic computing brain-inspired",        "Advanced Computing"),
    ("high performance computing parallel",          "Advanced Computing"),

    ("satellite remote sensing earth observation",   "Aerospace & Defense"),
    ("small satellite CubeSat launch vehicle",       "Aerospace & Defense"),
    ("autonomous drone UAV navigation",              "Aerospace & Defense"),
    ("hypersonic vehicle propulsion",                "Aerospace & Defense"),

    ("precision agriculture crop yield sensor",      "Agriculture Technology"),
    ("food security sustainable agriculture",        "Agriculture Technology"),
    ("controlled environment agriculture vertical",  "Agriculture Technology"),

    ("blockchain decentralized finance",             "Fintech"),
]

def _configure_openalex() -> None:

    key   = os.getenv("OPENALEX_API_KEY", "").strip()
    email = os.getenv("OPENALEX_EMAIL", "findout@example.com")

    if key:
        pyalex.config.api_key = key
        logger.debug("OpenAlex: API key set from OPENALEX_API_KEY")
    else:
        logger.debug("OpenAlex: no API key — using polite pool via email only")

    pyalex.config.email                = email
    pyalex.config.max_retries          = 3
    pyalex.config.retry_backoff_factor = 0.5

def _reconstruct_abstract(inverted_index: Optional[dict]) -> str:
    if not inverted_index:
        return ""
    try:
        pos_word: dict[int, str] = {}
        for word, positions in inverted_index.items():
            for pos in positions:
                pos_word[pos] = word
        return " ".join(pos_word[p] for p in sorted(pos_word))
    except Exception:
        return ""

def _get_abstract(work: dict) -> str:

    if work.get("abstract"):
        return str(work["abstract"])

    inv = work.get("abstract_inverted_index")
    if inv:
        reconstructed = _reconstruct_abstract(inv)
        if reconstructed:
            return reconstructed

    if work.get("_abstract_text"):
        return str(work["_abstract_text"])
    return ""

def _pubmed_delay(ncbi_key: Optional[str]) -> float:
    return PUBMED_DELAY_KEY if ncbi_key else PUBMED_DELAY

def _fetch_pubmed_abstract(title: str, ncbi_key: Optional[str] = None) -> str:
    if not title or len(title.strip()) < 10:
        return ""

    delay = _pubmed_delay(ncbi_key)

    search_params: dict = {
        "db":      "pubmed",
        "term":    f"{title[:100]}[Title]",
        "retmax":  "1",
        "retmode": "json",
    }
    if ncbi_key:
        search_params["api_key"] = ncbi_key

    try:
        resp   = httpx.get(PUBMED_SEARCH_URL, params=search_params, timeout=20)
        resp.raise_for_status()
        result = resp.json()
        ids    = result.get("esearchresult", {}).get("idlist", [])
        if not ids:
            return ""
        pmid = ids[0]
    except Exception as e:
        logger.debug(f"PubMed search failed for '{title[:50]}': {e}")
        return ""

    time.sleep(delay)

    fetch_params: dict = {
        "db":      "pubmed",
        "id":      pmid,
        "rettype": "abstract",
        "retmode": "text",
    }
    if ncbi_key:
        fetch_params["api_key"] = ncbi_key

    try:
        resp = httpx.get(PUBMED_FETCH_URL, params=fetch_params, timeout=20)
        resp.raise_for_status()
        text = resp.text.strip()
        if text and text.lower() not in ("", "null", "none"):
            return text[:5000]
    except Exception as e:
        logger.debug(f"PubMed fetch failed for PMID {pmid}: {e}")

    return ""

def _stable_id(work: dict) -> str:
    # 'https://openalex.org/W1234567890'
   # W-number: 'research_W1234567890'
    raw_id = work.get("id", "")
    short  = raw_id.split("/")[-1] if "/" in raw_id else raw_id
    return f"research_{short}"

def _extract_authors(work: dict, max_authors: int = 5) -> list[str]:
    authors = []
    for a in (work.get("authorships") or [])[:max_authors]:
        name = (a.get("author") or {}).get("display_name", "")
        if name:
            authors.append(name)
    return authors

def _extract_institutions(work: dict, max_inst: int = 5) -> list[str]:
    seen  = set()
    insts = []
    for a in (work.get("authorships") or []):
        for inst in (a.get("institutions") or [])[:2]:
            name = inst.get("display_name", "")
            if name and name not in seen:
                seen.add(name)
                insts.append(name)
            if len(insts) >= max_inst:
                return insts
    return insts

def _primary_institution(work: dict) -> str:
    insts = _extract_institutions(work, max_inst=1)
    return insts[0] if insts else ""

def _build_full_text(work: dict, abstract: str) -> str:
    parts: list[str] = []

    title = work.get("display_name") or work.get("title") or ""
    if title:
        parts.append(title)

    if abstract:
        parts.append(abstract)

    for t in (work.get("topics") or [])[:8]:
        name = t.get("display_name", "")
        if name:
            parts.append(name)

    for c in (work.get("concepts") or [])[:10]:
        name = c.get("display_name", "")
        if name:
            parts.append(name)

    for k in (work.get("keywords") or [])[:10]:
        kw = k.get("keyword", "")
        if kw:
            parts.append(kw)

    for inst in _extract_institutions(work, max_inst=3):
        parts.append(inst)

    return " ".join(p for p in parts if p).strip()

def _build_key_fields(work: dict, abstract: str) -> dict:
    oa        = work.get("open_access") or {}
    grants_oa = work.get("grants") or []

    return {
        "openalex_id":          work.get("id"),
        "doi":                  work.get("doi"),
        "publication_year":     work.get("publication_year"),
        "publication_date":     work.get("publication_date"),
        "type":                 work.get("type"),
        "language":             work.get("language"),
        "cited_by_count":       work.get("cited_by_count", 0),
        "referenced_works_count": work.get("referenced_works_count", 0),
        "is_open_access":       oa.get("is_oa", False),
        "oa_status":            oa.get("oa_status"),
        "pdf_url":              oa.get("oa_url"),
        "authors":              _extract_authors(work),
        "institutions":         _extract_institutions(work),
        "topics":               [t.get("display_name") for t in (work.get("topics") or [])[:8]
                                 if t.get("display_name")],
        "concepts":             [c.get("display_name") for c in (work.get("concepts") or [])[:10]
                                 if c.get("display_name")],
        "keywords":             [k.get("keyword") for k in (work.get("keywords") or [])[:10]
                                 if k.get("keyword")],
        "funding_sources":      [g.get("funder_display_name") for g in grants_oa
                                 if g.get("funder_display_name")],
        "abstract_source":      (
            "raw"         if work.get("abstract") else
            "inverted"    if work.get("abstract_inverted_index") else
            "pubmed"      if work.get("_pubmed_abstract") else
            "none"
        ),
    }

def fetch_research(
    topics: list[tuple[str, str]] = RESEARCH_TOPICS,
    max_per_topic: int = MAX_PER_TOPIC,
    min_year: int = MIN_YEAR,
    pubmed_fallback: bool = True,
) -> list[dict]:
    _configure_openalex()

    ncbi_key = os.getenv("NCBI_API_KEY", "").strip() or None
    if pubmed_fallback:
        if ncbi_key:
            logger.info(f"PubMed fallback: enabled (NCBI_API_KEY set, 10 req/s)")
        else:
            logger.info(f"PubMed fallback: enabled (no NCBI_API_KEY, 3 req/s — "
                        f"set NCBI_API_KEY for faster fallback)")
    else:
        logger.info("PubMed fallback: disabled")

    seen:    set[str]   = set()
    results: list[dict] = []

    pubmed_used  = 0
    abstract_missing = 0

    for query, sector in topics:
        logger.info(f"Research: '{query}' ({sector})...")

        try:
            works = (
                Works()
                .filter(
                    title={"search": query},
                    publication_year=f">{min_year - 1}",
                )
                .sort(cited_by_count="desc")
                .get(per_page=max_per_topic)
            )
        except Exception as e:
            logger.error(f"  OpenAlex error for '{query}': {e}")
            continue

        added = 0
        for work in works:
            uid = _stable_id(work)

            if uid in seen:

                for existing in results:
                    if existing.get("_foip_id") == uid:
                        qs = existing.get("_queries", [existing.get("_query", "")])
                        if query not in qs:
                            qs.append(query)
                        existing["_queries"] = qs
                        break
                continue
            seen.add(uid)

            abstract = _get_abstract(work)

            pubmed_abstract = ""
            if not abstract and pubmed_fallback:
                title = work.get("display_name") or work.get("title") or ""
                if title:
                    pubmed_abstract = _fetch_pubmed_abstract(title, ncbi_key)
                    if pubmed_abstract:
                        abstract = pubmed_abstract
                        pubmed_used += 1
                        logger.debug(f"  PubMed fallback: '{title[:60]}'")

            if not abstract:
                abstract_missing += 1

            work["_foip_id"]        = uid
            work["_sector"]         = sector
            work["_query"]          = query
            work["_queries"]        = [query]
            work["_abstract_text"]  = abstract
            work["_pubmed_abstract"]= pubmed_abstract
            work["_full_text"]      = _build_full_text(work, abstract)
            work["_key_fields"]     = _build_key_fields(work, abstract)

            results.append(work)
            added += 1

        logger.debug(f"  +{added} new unique (running total: {len(results)})")

    logger.success(
        f"Research: {len(results)} unique papers across {len(topics)} topics | "
        f"PubMed fallbacks used: {pubmed_used} | "
        f"No abstract: {abstract_missing}"
    )
    return results

def save_raw(data: list[dict]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out = RAW_DIR / f"{date.today()}.json"
    out.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"Raw research saved → {out} ({len(data)} records)")
    return out

def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No raw research files found in {RAW_DIR}")
    logger.info(f"Loading from disk: {files[-1].name}")
    return json.loads(files[-1].read_text())

def _probe() -> bool:
    print("\nProbing research collector APIs...")

    _configure_openalex()
    ncbi_key = os.getenv("NCBI_API_KEY", "").strip() or None

    print(f"\n  API keys:")
    print(f"    OPENALEX_API_KEY: {'set' if os.getenv('OPENALEX_API_KEY') else 'not set (anonymous polite pool)'}")
    print(f"    NCBI_API_KEY:     {'set — 10 req/s' if ncbi_key else 'not set — 3 req/s (get free key at ncbi.nlm.nih.gov/account)'}")

    all_ok = True

    print("\n   OpenAlex search API")
    try:
        works = (
            Works()
            .filter(
                title={"search": "large language model transformer"},
                publication_year=f">{MIN_YEAR - 1}",
            )
            .sort(cited_by_count="desc")
            .get(per_page=2)
        )
        if not works:
            print("  0 results returned")
            return False
        print(f"  [PASS] {len(works)} papers returned")
    except Exception as e:
        print(f"   OpenAlex error: {e}")
        return False

    w     = works[0]
    uid   = _stable_id(w)
    title = w.get("display_name") or w.get("title") or ""

    print(f"\n  Sample paper:")
    print(f"    id:          {uid}")
    print(f"    title:       {title[:80]}")
    print(f"    year:        {w.get('publication_year')}")
    print(f"    cited_by:    {w.get('cited_by_count', 0)}")
    print(f"    topics:      {len(w.get('topics') or [])} | "
          f"concepts: {len(w.get('concepts') or [])} | "
          f"authors: {len(w.get('authorships') or [])}")

    print("\n   Abstract reconstruction")
    abstract = _get_abstract(w)
    if abstract:
        source = ("raw abstract" if w.get("abstract") else
                  "inverted index reconstruction" if w.get("abstract_inverted_index")
                  else "pre-computed")
        print(f"         Abstract available via {source}: {len(abstract)} chars")
        print(f"  Preview: '{abstract[:180]}...'")
    else:
        print(f"         No abstract on this paper (common for older/non-OA papers)")
        print(f"         has_abstract_field: {bool(w.get('abstract'))}")
        print(f"         has_inverted_index: {bool(w.get('abstract_inverted_index'))}")

    print("\n  PubMed fallback (using NCBI_API_KEY if set)")
    pubmed_test = "mRNA vaccine immunotherapy"
    try:

        pubmed_text = _fetch_pubmed_abstract(pubmed_test, ncbi_key)
        if pubmed_text:
            print(f"  PubMed returned {len(pubmed_text)} chars")
            print(f"  Preview: '{pubmed_text[:180]}...'")
        else:
            print(f"      PubMed returned empty for '{pubmed_test}'")
            print(f"         (title mismatch is common — not necessarily broken)")
    except Exception as e:
        print(f"  PubMed error: {e}")

    print("\n  Full text assembly")
    full_text = _build_full_text(w, abstract)
    print(f"  full_text length: {len(full_text)} chars")
    if len(full_text) < 30:
        print("  Full text too short — something is wrong")
        all_ok = False
    elif len(full_text) < 100:
        print("  Full text is short — abstract may be missing")
    else:
        print("  Full text looks good")

    print("\n  key_fields assembly")
    kf = _build_key_fields(w, abstract)
    for field, value in kf.items():
        filled = bool(value) if not isinstance(value, bool) else value
        status = "[PASS]" if filled else "[WARN]"
        display = str(value)[:60] if value else "(empty)"
        print(f"  {status} {field:<26} {display}")

    print("\n  [6] Biomedical topic check")
    try:
        bio = (
            Works()
            .filter(
                title={"search": "CRISPR gene editing genomics"},
                publication_year=f">{MIN_YEAR - 1}",
            )
            .sort(cited_by_count="desc")
            .get(per_page=1)
        )
        if bio:
            bw = bio[0]
            b_title    = bw.get("display_name", "")[:70]
            b_abstract = _get_abstract(bw)
            print(f"  Biotechnology topic returns results")
            print(f"  title:    {b_title}")
            print(f"  abstract: {len(b_abstract)} chars")
        else:
            print(f"  0 results for CRISPR topic")
    except Exception as e:
        print(f"  Biomedical topic error: {e}")

    print(f"\n{'─'*55}")
    if all_ok:
        est_total = len(RESEARCH_TOPICS) * MAX_PER_TOPIC
        est_unique = est_total // 2   

        print(f"  esearch collector is ready.")
        print(f"  Topics:           {len(RESEARCH_TOPICS)}")
        print(f"  Max per topic:    {MAX_PER_TOPIC}")
        print(f"  Est. raw records: ~{est_total:,}")
        print(f"  Est. after dedup: ~{est_unique:,}")
        print(f"  Min year:         {MIN_YEAR}")
    else:
        print("  Some checks failed — review above.")
    return all_ok

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    from storage.db import get_db
    from storage.schema import create_schema
    from storage.writer import write_records

    parser = argparse.ArgumentParser(
        description="OpenAlex research paper collector with PubMed fallback",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--probe",     action="store_true",
                        help="Verify APIs with 2 topics and exit")
    parser.add_argument("--disk",      action="store_true",
                        help="Load from latest saved raw file instead of calling API")
    parser.add_argument("--max",       type=int, default=MAX_PER_TOPIC,
                        help=f"Max papers per topic (default {MAX_PER_TOPIC})")
    parser.add_argument("--year",      type=int, default=MIN_YEAR,
                        help=f"Only papers from this year onward (default {MIN_YEAR})")
    parser.add_argument("--no-pubmed", action="store_true",
                        help="Skip PubMed fallback (faster, fewer abstracts)")
    parser.add_argument("--limit",     type=int, default=None,
                        help="Only fetch the first N topics (for testing)")
    args = parser.parse_args()

    if args.probe:
        ok = _probe()
        sys.exit(0 if ok else 1)

    if args.disk:
        records = load_from_disk()
    else:
        topics  = RESEARCH_TOPICS[:args.limit] if args.limit else RESEARCH_TOPICS
        records = fetch_research(
            topics         = topics,
            max_per_topic  = args.max,
            min_year       = args.year,
            pubmed_fallback= not args.no_pubmed,
        )
        if records:
            save_raw(records)

    if records:
        con = get_db()
        create_schema(con)
        n = write_records(records, "research", con)
        logger.success(f"Done — {n} new research rows in DB.")
    else:
        logger.warning("No records to write.")

"""        
    python -m collectors.research_collector --max number           
    python -m collectors.research_collector --no-pubmed      
"""