import json
import os
import time
import argparse
from datetime import date
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

API_URL   = "https://search.patentsview.org/api/v1/patent/"
RAW_DIR   = Path(__file__).resolve().parent.parent / "data" / "raw" / "patents"
PAGE_SIZE = 100     

API_DELAY = 0.4     

SINCE_YEAR = 2022   

FIELDS = [
    "patent_id",
    "patent_title",
    "patent_abstract",
    "patent_date",
    "patent_type",
    "patent_number",
    "assignees.assignee_organization",
    "assignees.assignee_country",
    "assignees.assignee_type",
    "cpcs.cpc_subgroup_id",
    "cpcs.cpc_subgroup_title",
    "cpcs.cpc_group_id",
    "inventors.inventor_last_name",
    "inventors.inventor_first_name",
    "inventors.inventor_country",
    "applications.filing_date",
    "applications.series_code",
]

CPC_QUERIES: list[tuple[str, str]] = [

    ("G06N", "AI & Machine Learning"),    

    ("G06V", "AI & Machine Learning"),    

    ("G10L", "AI & Machine Learning"),    

    ("G06F", "Advanced Computing"),       

    ("G06Q", "Fintech"),                  

    ("G11C", "Advanced Computing"),       

    ("G06T", "Advanced Computing"),       

    ("H04L", "Cybersecurity"),            

    ("H04W", "Cybersecurity"),            

    ("G09C", "Cybersecurity"),            

    ("H04K", "Cybersecurity"),            

    ("A61B", "Health Technology"),        

    ("A61M", "Health Technology"),        

    ("A61P", "Health Technology"),        

    ("G01N", "Health Technology"),        

    ("G16H", "Health Technology"),        

    ("A61N", "Health Technology"),        

    ("C12N", "Biotechnology"),            

    ("A61K", "Biotechnology"),            

    ("C12Q", "Biotechnology"),            

    ("C40B", "Biotechnology"),            

    ("C07K", "Biotechnology"),            

    ("H01M", "Clean Energy"),             

    ("H02S", "Clean Energy"),             

    ("F03D", "Clean Energy"),             

    ("F24S", "Clean Energy"),             

    ("H02J", "Clean Energy"),             

    ("C25B", "Clean Energy"),             

    ("F17C", "Clean Energy"),             

    ("B01D", "Climate Technology"),       

    ("C10L", "Climate Technology"),       

    ("F01K", "Climate Technology"),       

    ("G06N3050", "Quantum Computing"),    

    ("H04B10", "Quantum Computing"),      

    ("G01R", "Quantum Computing"),        

    ("B33Y", "Advanced Manufacturing"),   

    ("B22F", "Advanced Manufacturing"),   

    ("B25J", "Advanced Manufacturing"),   

    ("G05B", "Advanced Manufacturing"),   

    ("B60W", "Advanced Manufacturing"),   

    ("G08G", "Advanced Manufacturing"),   

    ("H01L", "Advanced Computing"),       

    ("H10B", "Advanced Computing"),       

    ("H10K", "Advanced Computing"),       

    ("G02B", "Advanced Computing"),       

    ("H01S", "Advanced Computing"),       

    ("B82Y", "Advanced Computing"),       

    ("B64G", "Aerospace & Defense"),      

    ("B64C", "Aerospace & Defense"),      

    ("F42B", "Aerospace & Defense"),      

    ("H04B7", "Aerospace & Defense"),     

    ("A01B", "Agriculture Technology"),   

    ("A01G", "Agriculture Technology"),   

    ("A01N", "Agriculture Technology"),   

    ("G01S", "Agriculture Technology"),   

    ("A23L", "Agriculture Technology"),   

    ("B60L", "Transportation"),           

    ("B60K", "Transportation"),           

    ("E01C", "Infrastructure"),           

    ("H04Q", "Infrastructure"),           

    ("F24F", "Clean Energy"),             

    ("E04B", "Clean Energy"),             

    ("H05B", "Clean Energy"),             

    ("G07F", "Fintech"),                  

    ("H04L9", "Fintech"),                 

]

def _get_headers() -> dict:
    key = os.getenv("PATENTSVIEW_API_KEY", "").strip()
    h   = {"Content-Type": "application/json", "Accept": "application/json"}
    if key:
        h["X-Api-Key"] = key
    return h

def _fetch_page(cpc_prefix: str, after: Optional[str]) -> dict:
    options: dict = {
        "per_page": PAGE_SIZE,
        "sort":     [{"patent_date": "desc"}],
    }
    if after:
        options["after"] = after

    payload = {
        "q": {"_begins": {"cpc_subgroup_id": cpc_prefix}},
        "f": FIELDS,
        "o": options,
    }
    resp = httpx.post(API_URL, json=payload, headers=_get_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()

def _stable_id(patent: dict) -> str:
    return "patent_" + str(patent.get("patent_id", ""))

def _extract_assignees(patent: dict) -> tuple[str, str]:
    assignees = patent.get("assignees") or []
    if not assignees:
        return "", "US"

    orgs = [
        a.get("assignee_organization", "")
        for a in assignees[:3]
        if a.get("assignee_organization")
    ]
    country = assignees[0].get("assignee_country") or "US"
    return "; ".join(orgs), country

def _extract_inventors(patent: dict, max_inv: int = 5) -> list[str]:
    inventors = patent.get("inventors") or []
    names = []
    for inv in inventors[:max_inv]:
        first = inv.get("inventor_first_name", "")
        last  = inv.get("inventor_last_name", "")
        full  = f"{first} {last}".strip()
        if full:
            names.append(full)
    return names

def _extract_cpc_info(patent: dict) -> tuple[list[str], str]:
    cpcs = patent.get("cpcs") or []
    ids    = [c.get("cpc_subgroup_id", "") for c in cpcs if c.get("cpc_subgroup_id")]
    titles = [c.get("cpc_subgroup_title", "") for c in cpcs[:5]
              if c.get("cpc_subgroup_title")]
    return ids, "; ".join(titles)

def _is_recent(patent: dict, since_year: int) -> bool:
    d = patent.get("patent_date", "")
    if not d:
        return True  

    try:
        return int(str(d)[:4]) >= since_year
    except (ValueError, TypeError):
        return True

def _build_full_text(patent: dict) -> str:
    assignee_org, _ = _extract_assignees(patent)
    _, cpc_titles   = _extract_cpc_info(patent)

    parts = [
        patent.get("patent_title", ""),
        patent.get("patent_abstract", ""),
        cpc_titles,
        assignee_org,
        patent.get("_sector", ""),
        patent.get("_cpc_prefix", ""),
    ]
    return " ".join(p for p in parts if p and isinstance(p, str)).strip()

def _build_key_fields(patent: dict) -> dict:
    assignee_org, assignee_country = _extract_assignees(patent)
    cpc_ids, cpc_titles            = _extract_cpc_info(patent)
    inventors                      = _extract_inventors(patent)
    applications                   = patent.get("applications") or []
    filing_date = applications[0].get("filing_date", "") if applications else ""

    return {
        "patent_id":        patent.get("patent_id"),
        "patent_number":    patent.get("patent_number"),
        "patent_type":      patent.get("patent_type"),
        "patent_date":      patent.get("patent_date"),
        "filing_date":      filing_date,
        "assignee_org":     assignee_org,
        "assignee_country": assignee_country,
        "inventors":        inventors,
        "cpc_codes":        cpc_ids[:10],
        "cpc_titles":       cpc_titles,
        "cpc_prefix":       patent.get("_cpc_prefix", ""),
        "abstract_length":  len(patent.get("patent_abstract") or ""),
    }

def fetch_patents(
    cpc_queries: list[tuple[str, str]] = CPC_QUERIES,
    max_per_cpc: int = 200,
    since_year: int = SINCE_YEAR,
) -> list[dict]:
    api_key = os.getenv("PATENTSVIEW_API_KEY", "").strip()
    if api_key:
        logger.info("PatentsView: using API key from PATENTSVIEW_API_KEY")
    else:
        logger.info("PatentsView: no API key — using anonymous rate limits")

    seen:    set[str]   = set()
    results: list[dict] = []

    no_abstract_count = 0

    for cpc_prefix, sector in cpc_queries:
        logger.info(f"Patents: CPC {cpc_prefix} ({sector})...")
        after   = None
        fetched = 0

        while fetched < max_per_cpc:
            try:
                data    = _fetch_page(cpc_prefix, after)
                patents = data.get("patents") or []
                total   = data.get("total_patent_count", 0)

                if not patents:
                    break

                for p in patents:
                    if not _is_recent(p, since_year):
                        continue

                    uid = _stable_id(p)

                    if uid in seen:
                        for existing in results:
                            if existing.get("_foip_id") == uid:
                                prefixes = existing.get("_cpc_prefixes",
                                                        [existing.get("_cpc_prefix", "")])
                                if cpc_prefix not in prefixes:
                                    prefixes.append(cpc_prefix)
                                existing["_cpc_prefixes"] = prefixes
                                break
                        continue
                    seen.add(uid)

                    p["_foip_id"]      = uid
                    p["_sector"]       = sector
                    p["_cpc_prefix"]   = cpc_prefix
                    p["_cpc_prefixes"] = [cpc_prefix]
                    p["_full_text"]    = _build_full_text(p)
                    p["_key_fields"]   = _build_key_fields(p)

                    if not p.get("patent_abstract"):
                        no_abstract_count += 1

                    results.append(p)
                    fetched += 1

                after = data.get("after")
                if not after or len(patents) < PAGE_SIZE or fetched >= min(total, max_per_cpc):
                    break

                time.sleep(API_DELAY)

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    logger.warning(f"PatentsView rate limit for {cpc_prefix} — waiting 60s...")
                    time.sleep(60)
                    continue
                if e.response.status_code == 503:
                    logger.warning(f"PatentsView 503 for {cpc_prefix} — API may be down, skipping")
                    break
                logger.error(f"PatentsView HTTP {e.response.status_code} for {cpc_prefix}: {e}")
                break
            except httpx.HTTPError as e:
                logger.error(f"PatentsView network error for {cpc_prefix}: {e}")
                break

        cpc_unique = sum(1 for r in results if r.get("_cpc_prefix") == cpc_prefix)
        logger.debug(f"  CPC {cpc_prefix}: {cpc_unique} new unique (total: {len(results)})")

    logger.success(
        f"Patents: {len(results)} unique records across {len(cpc_queries)} CPC codes "
        f"(no abstract: {no_abstract_count})"
    )
    return results

def save_raw(data: list[dict]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out = RAW_DIR / f"{date.today()}.json"
    out.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"Raw patents saved → {out} ({len(data)} records)")
    return out

def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No raw patents files found in {RAW_DIR}")
    logger.info(f"Loading from disk: {files[-1].name}")
    return json.loads(files[-1].read_text())

def _probe() -> bool:
    print("\nProbing PatentsView API...")

    api_key = os.getenv("PATENTSVIEW_API_KEY", "").strip()
    print(f"\n  API key: {'set (higher rate limits)' if api_key else 'not set (anonymous)'}")
    print(f"  Endpoint: {API_URL}")

    print("\n  API — fetching 2 AI/ML patents (CPC G06N)...")
    try:
        data    = _fetch_page("G06N", after=None)
        patents = data.get("patents") or []
        total   = data.get("total_patent_count", 0)
        print(f"  API reachable — {total:,} total G06N patents")
        if not patents:
            print("  0 patents returned — API may be down or migrating")
            return False
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 503:
            print("         503 — PatentsView API is currently unavailable")
            print("         This is a known intermittent issue. Try again tomorrow.")
            print("         Patents source will be skipped until resolved.")
            return False
        print(f"   HTTP {e.response.status_code}: {e}")
        return False
    except Exception as e:
        print(f"  [FAIL] {e}")
        return False

    p = patents[0]
    print(f"\n  Sample patent field")
    print(f"    patent_id:     {p.get('patent_id', '')}")
    print(f"    patent_number: {p.get('patent_number', '')}")
    print(f"    patent_type:   {p.get('patent_type', '')}")
    print(f"    patent_date:   {p.get('patent_date', '')}")
    print(f"    title:         {(p.get('patent_title') or '')[:80]}")

    abstract = p.get("patent_abstract", "")
    if abstract:
        print(f"    abstract:      {len(abstract)} chars")
        print(f"    preview:       '{abstract[:150]}...'")
    else:
        print(f"    abstract:      (empty — common for some patent types)")

    assignee_org, assignee_country = _extract_assignees(p)
    print(f"    assignee_org:  {assignee_org[:70]}")
    print(f"    country:       {assignee_country}")

    inventors = _extract_inventors(p)
    print(f"    inventors:     {inventors[:3]}")

    cpc_ids, cpc_titles = _extract_cpc_info(p)
    print(f"    cpc_codes:     {cpc_ids[:4]}")
    print(f"    cpc_titles:    {cpc_titles[:100]}")

    p["_sector"]     = "AI & Machine Learning"
    p["_cpc_prefix"] = "G06N"
    p["_foip_id"]    = _stable_id(p)
    full_text        = _build_full_text(p)
    print(f"\n  [3] Full text assembly")
    print(f"    full_text length: {len(full_text)} chars")
    if len(full_text) < 20:
        print("    [FAIL] Full text too short")
        return False
    print(f"    preview: '{full_text[:200]}...'")
    print(f"    Full text OK")

    kf = _build_key_fields(p)
    print(f"\n  key_fields assembly")
    for field, value in kf.items():
        filled = bool(value) if not isinstance(value, bool) else True
        status = "[PASS]" if filled else "[WARN]"
        display = str(value)[:70] if value else "(empty)"
        print(f"    {status} {field:<20} {display}")

    print(f"\n   Dedup check — fetching 1 patent from CPC H04L (Cybersecurity)")
    try:
        data2   = _fetch_page("H04L", after=None)
        p2_list = data2.get("patents") or []
        total2  = data2.get("total_patent_count", 0)
        print(f"    H04L returned {total2:,} total cybersecurity patents")
    except Exception as e:
        print(f"  H04L fetch failed: {e}")

    print(f"\n  [6] Date filter (since {SINCE_YEAR})")
    recent = _is_recent(p, SINCE_YEAR)
    pdate  = p.get("patent_date", "unknown")
    print(f"    Patent date: {pdate} → {'[included' if recent else ' would be filtered'}")

    print(f"\n{'─'*55}")
    print(f"  works.")
    print(f"\n  CPC codes configured: {len(CPC_QUERIES)}")
    print(f"  Max per CPC (default): {200}")
    print(f"  Est. raw records:    ~{len(CPC_QUERIES) * 200:,}")
    print(f"  Est. after dedup:    ~{len(CPC_QUERIES) * 200 // 3:,} (high overlap between codes)")
    print(f"  Since year:          {SINCE_YEAR}")
    return True

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    from storage.db import get_db
    from storage.schema import create_schema
    from storage.writer import write_records

    parser = argparse.ArgumentParser(
        description="PatentsView patent collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--probe",  action="store_true",
                        help="Test API with 2 CPC codes and exit")
    parser.add_argument("--disk",   action="store_true",
                        help="Load from latest saved raw file")
    parser.add_argument("--max",    type=int, default=200,
                        help=f"Max patents per CPC code (default 200)")
    parser.add_argument("--year",   type=int, default=SINCE_YEAR,
                        help=f"Only patents from this year onward (default {SINCE_YEAR})")
    parser.add_argument("--limit",  type=int, default=None,
                        help="Only fetch the first N CPC codes (for testing)")
    args = parser.parse_args()

    if args.probe:
        ok = _probe()
        sys.exit(0 if ok else 1)

    if args.disk:
        records = load_from_disk()
    else:
        queries = CPC_QUERIES[:args.limit] if args.limit else CPC_QUERIES
        records = fetch_patents(
            cpc_queries = queries,
            max_per_cpc = args.max,
            since_year  = args.year,
        )
        if records:
            save_raw(records)
        else:
            logger.warning("No patents returned — API down.")

    if records:
        con = get_db()
        create_schema(con)
        n = write_records(records, "patents", con)
        logger.success(f"Done — {n} new patent rows in DB.")
    else:
        logger.warning("No records to write.")

"""
    python -m collectors.patents_collector --probe
    python -m collectors.patents_collector --max number

"""