import hashlib
import json
import time
import argparse
from datetime import date
from pathlib import Path
from typing import Optional

import httpx
from loguru import logger

SEARCH_URL   = "https://api.grants.gov/v1/api/search2"
DETAIL_URL   = "https://api.grants.gov/v1/api/fetchOpportunity"
RAW_DIR      = Path(__file__).resolve().parent.parent / "data" / "raw" / "grants"
PAGE_SIZE    = 25      

DETAIL_DELAY = 0.3     

MAX_PER_KEYWORD = 500  

OPP_STATUSES = "forecasted|posted"

SEARCH_KEYWORDS = [

    "large language model",
    "generative AI",
    "foundation model",
    "explainable AI",
    "AI safety",
    "AI trustworthiness",
    "artificial intelligence",
    "machine learning",
    "deep learning",
    "computer vision",
    "natural language processing",
    "federated learning",
    "reinforcement learning",

    "zero trust security",
    "critical infrastructure protection",
    "adversarial machine learning",
    "post-quantum cryptography",
    "privacy preserving technology",
    "network security",
    "cybersecurity",
    "cyber resilience",

    "hydrogen fuel cell",
    "long duration energy storage",
    "energy storage battery",
    "solar photovoltaic",
    "offshore wind energy",
    "grid modernization",
    "smart grid",
    "nuclear fusion",
    "advanced nuclear reactor",
    "geothermal energy",
    "microgrid technology",
    "clean energy",
    "renewable energy",

    "carbon capture utilization",
    "carbon sequestration",
    "climate change adaptation",
    "climate resilience",
    "net zero emissions",
    "flood resilience",
    "wildfire technology",
    "drought resilience",
    "climate technology",
    "environmental remediation",

    "CRISPR gene editing",
    "synthetic biology",
    "mRNA therapeutics",
    "cell therapy",
    "protein structure prediction",
    "microbiome research",
    "drug discovery AI",
    "gene therapy",
    "biomanufacturing",
    "biodefense",
    "biotechnology",

    "medical imaging AI",
    "wearable health monitoring",
    "digital therapeutics",
    "telehealth",
    "precision medicine",
    "mental health technology",
    "substance use disorder technology",
    "maternal health",
    "health equity technology",
    "pandemic preparedness",
    "infectious disease surveillance",
    "opioid treatment technology",
    "digital health",
    "medical device",

    "quantum computing",
    "quantum sensing",
    "quantum networking",
    "quantum cryptography",
    "quantum information science",

    "additive manufacturing",
    "advanced robotics",
    "autonomous vehicle",
    "industrial IoT",
    "advanced materials",
    "composite materials",
    "advanced manufacturing",
    "supply chain resilience",
    "domestic manufacturing",

    "semiconductor manufacturing",
    "neuromorphic computing",
    "edge computing",
    "photonics",
    "high performance computing",
    "microelectronics",
    "printed electronics",

    "small satellite",
    "autonomous drone",
    "hypersonic technology",
    "directed energy",
    "space technology",
    "defense technology",
    "dual use technology",

    "precision agriculture",
    "agricultural biotechnology",
    "controlled environment agriculture",
    "food safety technology",
    "food systems technology",
    "water technology",
    "rural development technology",
    "specialty crop research",

    "transportation safety technology",
    "electric vehicle infrastructure",
    "autonomous transportation",
    "bridge infrastructure technology",
    "broadband infrastructure",
    "port modernization",

    "building energy efficiency",
    "advanced HVAC",
    "smart building technology",
    "energy efficient manufacturing",

    "workforce development technology",
    "STEM education technology",
    "apprenticeship technology",
    "skills training technology",

    "affordable housing technology",
    "community resilience",
    "disaster preparedness technology",
    "environmental justice",

    "financial technology",
    "decentralized finance",
    "small business innovation",
    "technology commercialization",
]

def _stable_id(opp: dict) -> str:
    opp_int_id = opp.get("id")
    if opp_int_id:
        return f"grants_{opp_int_id}"

    raw = opp.get("number") or f"{opp.get('title','')}{opp.get('agency','')}"
    return "grants_" + hashlib.md5(raw.encode()).hexdigest()[:16]

def _get_opportunity_id(opp: dict) -> Optional[int]:
    val = opp.get("id")
    if val is not None:
        try:
            return int(val)
        except (ValueError, TypeError):
            pass
    return None

def _fetch_page(keyword: str, start: int) -> dict:
    body = {
        "keyword":        keyword,
        "oppStatuses":    OPP_STATUSES,
        "rows":           PAGE_SIZE,
        "startRecordNum": start,
    }
    resp = httpx.post(SEARCH_URL, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()

def _fetch_detail(opportunity_id: int) -> Optional[dict]:
    try:
        resp = httpx.post(
            DETAIL_URL,
            json={"opportunityId": opportunity_id},
            timeout=30,
        )
        resp.raise_for_status()
        data     = resp.json()
        d        = data.get("data", {})
        detail   = dict(d)
        synopsis = d.get("synopsis", {})
        if isinstance(synopsis, dict):

            for k, v in synopsis.items():
                if k not in detail or not detail[k]:
                    detail[k] = v
        return detail if detail else None

    except httpx.HTTPStatusError as e:
        logger.debug(f"Detail fetch HTTP {e.response.status_code} "
                     f"for opportunityId={opportunity_id}")
        return None
    except Exception as e:
        logger.debug(f"Detail fetch failed for opportunityId={opportunity_id}: {e}")
        return None

def _build_full_text(opp: dict, detail: Optional[dict]) -> str:
    d = detail or {}
    parts = [
        opp.get("title", ""),

        opp.get("agency", ""),
        opp.get("agencyCode", ""),
        opp.get("number", ""),        

        d.get("synopsisDesc", ""),
        d.get("description", "") if isinstance(d.get("description"), str) else "",
        d.get("additionalInfo", ""),
        d.get("programDescription", ""),
        d.get("objectives", ""),
        d.get("eligibilities", "") if isinstance(d.get("eligibilities"), str) else "",
        d.get("costSharing", "") if isinstance(d.get("costSharing"), str) else "",
        d.get("cfda", ""),
        d.get("agencyName", ""),      

        opp.get("_keyword", ""),
    ]
    return " ".join(p for p in parts if p and isinstance(p, str)).strip()

def _merge_detail(opp: dict, detail: Optional[dict]) -> dict:
    if not detail:
        return opp
    merged = dict(opp)

    for key in ("synopsisDesc", "additionalInfo", "programDescription",
                "objectives", "cfdaNumbers", "programUrl",
                "agencyContactName", "agencyContactEmail",
                "agencyPhone", "agencyAddressDesc",
                "fundingInstruments", "applicantTypes",
                "fundingActivityCategories"):
        if detail.get(key) and not merged.get(key):
            merged[key] = detail[key]

    for key in ("awardCeiling", "awardFloor", "estimatedFunding",
                "expectedNumberOfAwards"):
        val = detail.get(key)
        if val is not None and val != "none" and val != "":
            merged[key] = val

    if detail.get("eligibilities"):
        merged["eligibilities"] = detail["eligibilities"]

    for key in ("postingDate", "responseDate", "archiveDate"):
        if detail.get(key) and not merged.get(key):
            merged[key] = detail[key]

    return merged

def fetch_grants(
    keywords: list[str] = SEARCH_KEYWORDS,
    max_per_keyword: int = MAX_PER_KEYWORD,
    fetch_detail: bool = True,
) -> list[dict]:
    seen:    set[str]   = set()
    results: list[dict] = []

    for keyword in keywords:
        logger.info(f"Grants: fetching '{keyword}'...")
        start = 0

        while True:
            try:
                data    = _fetch_page(keyword, start)
                hits    = data.get("data", {}).get("oppHits", [])
                total   = int(data.get("data", {}).get("hitCount", 0))

                if not hits:
                    logger.debug(f"  '{keyword}': 0 hits")
                    break

                logger.debug(f"  '{keyword}': {total} total, fetching from {start}")

                for opp in hits:
                    uid = _stable_id(opp)
                    if uid in seen:

                        for existing in results:
                            if existing.get("_foip_id") == uid:
                                kws = existing.get("_keywords", [existing.get("_keyword", "")])
                                if keyword not in kws:
                                    kws.append(keyword)
                                existing["_keywords"] = kws
                                break
                        continue
                    seen.add(uid)

                    opp["_foip_id"]  = uid
                    opp["_keyword"]  = keyword        

                    opp["_keywords"] = [keyword]      

                    if fetch_detail:
                        opp_int_id = _get_opportunity_id(opp)
                        if opp_int_id:
                            opp["_opportunity_id"] = opp_int_id
                            detail = _fetch_detail(opp_int_id)
                            if detail:
                                opp = _merge_detail(opp, detail)
                        time.sleep(DETAIL_DELAY)

                    opp["_full_text"] = _build_full_text(opp, None)
                    results.append(opp)

                start += PAGE_SIZE
                if start >= min(total, max_per_keyword):
                    break

            except httpx.HTTPError as e:
                logger.error(f"Grants HTTP error for '{keyword}': {e}")
                break

        kw_new = sum(1 for r in results if r.get("_keyword") == keyword)
        logger.debug(f"  '{keyword}': {total} results → {kw_new} new unique added "
                     f"(running total: {len(results)})")

    logger.success(f"Grants: {len(results)} unique records fetched + enriched "
                   f"across {len(keywords)} keywords.")
    return results

def save_raw(data: list[dict]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out = RAW_DIR / f"{date.today()}.json"
    out.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"Raw grants saved → {out} ({len(data)} records)")
    return out

def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No raw grants files in {RAW_DIR}")
    logger.info(f"Loading from disk: {files[-1].name}")
    return json.loads(files[-1].read_text())

def probe_detail_api() -> bool:
    print("\nProbing Grants.gov APIs...")

    try:
        data  = _fetch_page("artificial intelligence", 0)
        hits  = data.get("data", {}).get("oppHits", [])
        total = int(data.get("data", {}).get("hitCount", 0))
        print(f"  Search API: {total} results for 'artificial intelligence'")
        if total == 0:
            print("  [FAIL] Still 0 results — check oppStatuses or API availability")
            print(f"  Full response: {json.dumps(data, indent=2)[:500]}")
            return False
    except Exception as e:
        print(f"  [FAIL] Search API error: {e}")
        return False

    if not hits:
        print("  [FAIL] No hits in response")
        return False

    first = hits[0]
    print(f"\n  Listing fields: {list(first.keys())}")
    print(f"  id={first.get('id')} | agency='{first.get('agency','')}' | "
          f"title='{first.get('title','')[:60]}'")
    print(f"  openDate={first.get('openDate')} | closeDate={first.get('closeDate')} | "
          f"oppStatus={first.get('oppStatus')}")

    opp_int_id = _get_opportunity_id(first)
    if not opp_int_id:
        print("  [FAIL] No numeric id in listing")
        return False

    print(f"\n  Fetching detail for opportunityId={opp_int_id}...")
    detail = _fetch_detail(opp_int_id)
    if not detail:
        print(f"  [FAIL] Detail API returned nothing")
        return False

    print(f"  [PASS] Detail API works!")
    print(f"  Detail fields: {list(detail.keys())[:12]}")
    print(f"  synopsisDesc:  {len(detail.get('synopsisDesc',''))} chars")
    print(f"  awardCeiling:  {detail.get('awardCeiling')}")
    print(f"  awardFloor:    {detail.get('awardFloor')}")
    elig = detail.get("eligibilities", "")
    print(f"  eligibilities: {str(elig)[:100] if elig else '(empty)'}")
    print(f"  agencyName:    {detail.get('agencyName','')}")

    merged   = _merge_detail(first, detail)
    full_txt = _build_full_text(merged, None)
    print(f"\n  Full text preview ({len(full_txt)} chars total):")
    print(f"  '{full_txt[:200]}...'")
    return True

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    from storage.db import get_db
    from storage.schema import create_schema
    from storage.writer import write_records

    parser = argparse.ArgumentParser()
    parser.add_argument("--disk",      action="store_true",
                        help="Load from latest saved file")
    parser.add_argument("--no-detail", action="store_true",
                        help="Skip per-record detail fetch")
    parser.add_argument("--limit",     type=int, default=None,
                        help="Limit to first N keywords (for testing)")
    parser.add_argument("--probe",     action="store_true",
                        help="Test APIs with one record and exit")
    args = parser.parse_args()

    if args.probe:
        ok = probe_detail_api()
        sys.exit(0 if ok else 1)

    if args.disk:
        records = load_from_disk()
    else:
        kws     = SEARCH_KEYWORDS[:args.limit] if args.limit else SEARCH_KEYWORDS
        records = fetch_grants(keywords=kws, fetch_detail=not args.no_detail)
        save_raw(records)

    con = get_db()
    create_schema(con)
    n = write_records(records, "grants", con)
    logger.success(f"Done — {n} new grants rows in DB.")

"""
    python -m collectors.grants_collector --probe        
    python -m collectors.grants_collector --disk   
"""