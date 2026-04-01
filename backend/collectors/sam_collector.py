import json
import os
import re
import time
import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

SEARCH_URL  = "https://api.sam.gov/prod/opportunities/v2/search"
RAW_DIR     = Path(__file__).resolve().parent.parent / "data" / "raw" / "sam"
PAGE_SIZE   = 100    

DESC_DELAY  = 0.2    

PAGE_DELAY  = 0.1    

NOTICE_TYPES = "o,p,k,r,s"   

FETCH_DESC_FOR = {"o", "p", "k", "r", "s"}

SEARCH_KEYWORDS = [

    "artificial intelligence",
    "machine learning",
    "large language model",
    "computer vision",
    "natural language processing",
    "cybersecurity",
    "zero trust",
    "cloud computing",
    "data analytics",
    "software development",

    "autonomous systems",
    "hypersonic",
    "directed energy",
    "electronic warfare",
    "intelligence surveillance reconnaissance",
    "command control communications",
    "unmanned systems",
    "counter-drone",
    "biodefense",
    "nuclear security",

    "medical research",
    "biotechnology",
    "drug development",
    "medical device",
    "telehealth",
    "precision medicine",
    "genomics",
    "pandemic preparedness",

    "clean energy",
    "renewable energy",
    "energy storage",
    "hydrogen fuel",
    "nuclear energy",
    "carbon capture",
    "environmental remediation",
    "climate resilience",
    "smart grid",

    "advanced manufacturing",
    "additive manufacturing",
    "semiconductor",
    "microelectronics",
    "advanced materials",
    "robotics",
    "industrial automation",

    "space systems",
    "satellite",
    "launch vehicle",
    "propulsion systems",
    "hypersonics",

    "electric vehicle",
    "autonomous vehicle",
    "broadband",
    "bridge infrastructure",
    "transportation safety",

    "precision agriculture",
    "food safety",
    "agricultural technology",
    "water technology",

    "small business innovation research",
    "technology transfer",
    "STEM education",
    "workforce development",
    "research development test evaluation",
]

NAICS_CODES = [

    "541511", "541512", "541519", "518210",

    "541713", "541714", "541715", "541720",

    "541513", "541690", "541990",

    "237130", "221118", "221119", "541620",

    "621511", "541380", "325412", "334510",

    "333249", "336411", "336413", "336414",

    "334511", "334413", "334419",

    "541370", "311999",

    "488999", "336320",
]

def _get_api_key() -> str:
    key = os.getenv("SAM_API_KEY", "").strip()
    if not key:
        raise ValueError("SAM_API_KEY not found in .env")
    return key

def _date_str(d: date) -> str:
    return d.strftime("%m/%d/%Y")

def _extract_agency(opp: dict) -> str:
    full = opp.get("fullParentPathName", "")
    if not full:
        return ""
    parts = [p.strip().title() for p in full.split(".") if p.strip()]
    if len(parts) >= 2:
        return f"{parts[0]} — {parts[1]}"
    return parts[0] if parts else ""

def _extract_contact(opp: dict) -> str:
    contacts = opp.get("pointOfContact") or []
    if not contacts:
        return ""
    p = next((c for c in contacts if c.get("type") == "primary"), contacts[0])
    return " | ".join(filter(None, [
        p.get("fullName", ""), p.get("email", ""), p.get("phone", "")
    ]))

def _extract_funding(opp: dict) -> Optional[float]:
    amt = (opp.get("award") or {}).get("amount")
    if amt:
        try:
            return float(str(amt).replace(",", "").replace("$", "").strip())
        except (ValueError, TypeError):
            pass
    return None

def _should_fetch_desc(opp: dict) -> bool:
    t = (opp.get("type") or opp.get("baseType") or "").lower()
    base = (opp.get("baseType") or "").lower()

    if "award" in t or base == "a":
        return False
    return True

def _fetch_description(desc_url: str, api_key: str) -> str:
    if not desc_url or desc_url in ("null", "None", ""):
        return ""
    if "api.sam.gov" not in desc_url and "sam.gov" not in desc_url:
        return ""
    try:
        resp = httpx.get(
            desc_url,
            params={"api_key": api_key},
            timeout=20,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            return ""
        content = resp.text.strip()
        if not content or content.lower() in ("null", "none", ""):
            return ""
        if "<" in content:
            soup = BeautifulSoup(content, "html.parser")
            text = soup.get_text(separator=" ", strip=True)
            text = re.sub(r'\s+', ' ', text).strip()
            return text[:50_000]
        return content[:50_000]
    except Exception as e:
        logger.debug(f"SAM desc fetch failed ({desc_url[:60]}): {e}")
        return ""

def _build_full_text(opp: dict) -> str:
    parts = [
        opp.get("title", ""),
        opp.get("_description_text", ""),
        opp.get("fullParentPathName", ""),
        opp.get("typeOfSetAsideDescription", ""),
        str(opp.get("naicsCode", "")),
        opp.get("classificationCode", ""),
        opp.get("solicitationNumber", ""),
        str((opp.get("placeOfPerformance") or {}).get("state", {}).get("name", "")),
        str((opp.get("placeOfPerformance") or {}).get("city", {}).get("name", "")),
    ]
    return " ".join(p for p in parts if p and isinstance(p, str)).strip()

def _enrich_record(opp: dict, api_key: str, fetch_detail: bool) -> dict:
    desc_url = opp.get("description", "")
    if fetch_detail and desc_url and desc_url != "null" and _should_fetch_desc(opp):
        opp["_description_text"] = _fetch_description(desc_url, api_key)
        time.sleep(DESC_DELAY)
    else:
        opp["_description_text"] = ""

    opp["_agency"]    = _extract_agency(opp)
    opp["_contact"]   = _extract_contact(opp)
    opp["_funding"]   = _extract_funding(opp)
    opp["_full_text"] = _build_full_text(opp)
    return opp

def _search_by_keyword(api_key: str, keyword: str, posted_from: str,
                       posted_to: str, offset: int) -> dict:
    params = {
        "api_key":    api_key,
        "limit":      PAGE_SIZE,
        "offset":     offset,
        "q":          keyword,       

        "postedFrom": posted_from,
        "postedTo":   posted_to,
        "ptype":      NOTICE_TYPES,
        "active":     "Yes",
    }
    resp = httpx.get(SEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def _search_by_naics(api_key: str, naics: str, posted_from: str,
                     posted_to: str, offset: int) -> dict:
    params = {
        "api_key":    api_key,
        "limit":      PAGE_SIZE,
        "offset":     offset,
        "naicsCode":  naics,
        "postedFrom": posted_from,
        "postedTo":   posted_to,
        "ptype":      NOTICE_TYPES,
        "active":     "Yes",
    }
    resp = httpx.get(SEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_sam(
    days_back: int = 90,
    mode: str = "keywords",             

    fetch_detail: bool = True,
    max_per_term: int = 500,
    keywords: list[str] = None,
    naics_codes: list[str] = None,
) -> list[dict]:
    api_key     = _get_api_key()
    today       = date.today()
    posted_to   = _date_str(today)
    posted_from = _date_str(today - timedelta(days=days_back))

    seen:    set[str]   = set()
    results: list[dict] = []
    req_count = 0  

    search_terms = keywords or (SEARCH_KEYWORDS if mode == "keywords" else None)
    naics_list   = naics_codes or NAICS_CODES

    if mode == "keywords" and search_terms:
        logger.info(f"SAM: keyword mode — {len(search_terms)} keywords, "
                    f"last {days_back} days, detail={fetch_detail}")
        for kw in search_terms:
            logger.info(f"  Keyword: '{kw}'...")
            offset = 0
            kw_added = 0

            while True:
                try:
                    data  = _search_by_keyword(api_key, kw, posted_from, posted_to, offset)
                    req_count += 1
                    opps  = data.get("opportunitiesData", [])
                    total = int(data.get("totalRecords", 0))

                    if not opps:
                        break

                    for opp in opps:
                        nid = opp.get("noticeId", "")
                        if not nid or nid in seen:
                            continue
                        seen.add(nid)
                        opp["_keyword"] = kw
                        opp = _enrich_record(opp, api_key, fetch_detail)
                        if fetch_detail and opp.get("_description_text"):
                            req_count += 1
                        results.append(opp)
                        kw_added += 1

                    offset += PAGE_SIZE
                    if offset >= min(total, max_per_term):
                        break
                    time.sleep(PAGE_DELAY)

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        logger.error(
                            f"SAM rate limit hit after {req_count} requests. "
                            f"Got {len(results)} records. "
                            f"Quota resets at midnight UTC. "
                            f"Run again tomorrow or use --no-detail to reduce requests."
                        )
                        return results
                    logger.error(f"SAM HTTP {e.response.status_code} for '{kw}': {e}")
                    break
                except httpx.HTTPError as e:
                    logger.error(f"SAM network error for '{kw}': {e}")
                    break

            logger.debug(f"    '{kw}': +{kw_added} new unique (total: {len(results)}, "
                         f"~{req_count} API requests used)")

    else: 
        logger.info(f"SAM: NAICS mode — {len(naics_list)} codes, "
                    f"last {days_back} days, detail={fetch_detail}")
        for naics in naics_list:
            logger.info(f"  NAICS: {naics}...")
            offset = 0

            while True:
                try:
                    data  = _search_by_naics(api_key, naics, posted_from, posted_to, offset)
                    req_count += 1
                    opps  = data.get("opportunitiesData", [])
                    total = int(data.get("totalRecords", 0))

                    if not opps:
                        break

                    for opp in opps:
                        nid = opp.get("noticeId", "")
                        if not nid or nid in seen:
                            continue
                        seen.add(nid)
                        opp["_naics_queried"] = naics
                        opp = _enrich_record(opp, api_key, fetch_detail)
                        if fetch_detail and opp.get("_description_text"):
                            req_count += 1
                        results.append(opp)

                    offset += PAGE_SIZE
                    if offset >= min(total, max_per_term):
                        break
                    time.sleep(PAGE_DELAY)

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        logger.error(
                            f"SAM rate limit hit after {req_count} requests. "
                            f"Got {len(results)} records. "
                            f"Quota resets at midnight UTC."
                        )
                        return results
                    logger.error(f"SAM HTTP {e.response.status_code} for NAICS {naics}: {e}")
                    break
                except httpx.HTTPError as e:
                    logger.error(f"SAM network error for NAICS {naics}: {e}")
                    break

    logger.success(
        f"SAM: {len(results)} unique records fetched "
        f"(~{req_count} API requests used out of 1,000 daily budget)"
    )
    return results

def save_raw(data: list[dict]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out = RAW_DIR / f"{date.today()}.json"
    out.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"Raw SAM saved → {out} ({len(data)} records)")
    return out

def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No raw SAM files in {RAW_DIR}")
    logger.info(f"Loading from disk: {files[-1].name}")
    return json.loads(files[-1].read_text())

def probe() -> bool:
    api_key = _get_api_key()
    today   = date.today()
    print("\nProbing SAM.gov API (keyword mode)...")

    try:
        data  = _search_by_keyword(
            api_key, "artificial intelligence",
            _date_str(today - timedelta(days=30)), _date_str(today), 0
        )
        opps  = data.get("opportunitiesData", [])
        total = data.get("totalRecords", 0)
        print(f"  Search API: {total} results for 'artificial intelligence' (last 30 days)")
        if not opps:
            print("  0 results — try a longer date range or check API key")
            return False
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            print(f"  Rate limit hit (429) — quota exhausted for today.")
            print(f"  The limit is 1,000 requests/day and resets at midnight UTC.")
            print(f"  Try again tomorrow, or run with --no-detail to reduce requests.")
            return False
        print(f"  HTTP {e.response.status_code}: {e}")
        return False
    except Exception as e:
        print(f"  {e}")
        return False

    first = opps[0]
    agency = _extract_agency(first)
    print(f"\n  noticeId:        {first.get('noticeId','')}")
    print(f"  title:           {first.get('title','')[:70]}")
    print(f"  agency:          {agency}")
    print(f"  type:            {first.get('type','')}")
    print(f"  naicsCode:       {first.get('naicsCode','')}")
    print(f"  postedDate:      {first.get('postedDate','')}")
    print(f"  responseDeadLine:{first.get('responseDeadLine','')}")
    print(f"  setAside:        {first.get('typeOfSetAsideDescription','')}")
    print(f"  award.amount:    {(first.get('award') or {}).get('amount','N/A')}")

    desc_url = first.get("description", "")
    print(f"  description URL: {desc_url[:80] if desc_url and desc_url != 'null' else '(none)'}")

    if desc_url and desc_url not in ("null", "None", "") and _should_fetch_desc(first):
        print(f"\n  Fetching description text...")
        text = _fetch_description(desc_url, api_key)
        if text:
            print(f"  {len(text)} chars fetched")
            print(f"  Preview: '{text[:250]}...'")
        else:
            print(f"  Empty — this record may use attachments only")

    full = _build_full_text({**first, "_description_text": ""})
    print(f"\n  Full text (title+agency+metadata): {len(full)} chars")
    print(f"\n  works ")
    return True

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    from storage.db import get_db
    from storage.schema import create_schema
    from storage.writer import write_records

    parser = argparse.ArgumentParser(
        description="SAM.gov collector — respects 1,000 req/day limit"
    )
    parser.add_argument("--probe",     action="store_true")
    parser.add_argument("--disk",      action="store_true")
    parser.add_argument("--no-detail", action="store_true",
                        help="Skip description fetch (saves ~150-300 requests)")
    parser.add_argument("--mode",      choices=["keywords", "naics"],
                        default="keywords")
    parser.add_argument("--days",      type=int, default=90)
    args = parser.parse_args()

    if args.probe:
        ok = probe()
        sys.exit(0 if ok else 1)

    if args.disk:
        records = load_from_disk()
    else:
        records = fetch_sam(
            days_back    = args.days,
            mode         = args.mode,
            fetch_detail = not args.no_detail,
        )
        if records:
            save_raw(records)

    if records:
        con = get_db()
        create_schema(con)
        n = write_records(records, "sam", con)
        logger.success(f"Done — {n} new SAM rows in DB.")

"""
    python -m collectors.sam_collector --probe                   
    python -m collectors.sam_collector --mode naics       
    python -m collectors.sam_collector --no-detail        
    python -m collectors.sam_collector --disk             
    python -m collectors.sam_collector --days 60          
"""