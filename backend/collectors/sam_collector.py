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

SEARCH_URL  = "https://api.sam.gov/opportunities/v2/search"
RAW_DIR     = Path(__file__).resolve().parent.parent / "data" / "raw" / "sam"
PAGE_SIZE   = 100
PAGE_DELAY  = 180
NOTICE_TYPES = "o,p,k,r,s"

SEARCH_KEYWORDS = [
    "artificial intelligence",
    "machine learning",
    "cybersecurity",
    "cloud computing",
    "software development",
    "data analytics",
    "autonomous systems",
    "biodefense",
    "clean energy",
    "renewable energy",
    "advanced manufacturing",
    "semiconductor",
    "space systems",
    "medical research",
    "biotechnology",
    "quantum computing",
    "electric vehicle",
    "climate resilience",
    "small business innovation",
    "unmanned systems",
    "quantum computing",
    "precision agriculture",
    "telehealth",
    "carbon capture",
    "CRISPR",
    "drug discovery",
    "financial technology",
    "blockchain",
    "digital payments"

]

NAICS_CODES = [
    "541511", "541512", "541519",  
    "541713", "541714", "541715",  
    "334413", "334510",            
    "621511", "325412",          
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
        return opp.get("department", "") or ""
    parts = [p.strip().title() for p in full.split(".") if p.strip()]
    return f"{parts[0]} — {parts[1]}" if len(parts) >= 2 else (parts[0] if parts else "")


def _extract_funding(opp: dict) -> Optional[float]:
    amt = (opp.get("award") or {}).get("amount")
    if amt:
        try:
            return float(str(amt).replace(",", "").replace("$", "").strip())
        except (ValueError, TypeError):
            pass
    return None


def _fetch_description(desc_url: str, api_key: str) -> str:
    if not desc_url or desc_url in ("null", "None", ""):
        return ""
    try:
        resp = httpx.get(
            desc_url,
            params={"api_key": api_key},
            timeout=httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0),
            follow_redirects=True,
        )
        if resp.status_code != 200:
            return ""
        content = resp.text.strip()
        if not content or content.lower() in ("null", "none", ""):
            return ""
        if "<" in content:
            soup = BeautifulSoup(content, "html.parser")
            return soup.get_text(" ", strip=True)[:3000]
        return content[:3000]
    except Exception:
        return ""


def _build_full_text(opp: dict) -> str:
    parts = [
        opp.get("title", ""),
        opp.get("_agency", ""),
        opp.get("_description_text", ""),
        opp.get("_keyword", ""),
        opp.get("naicsCode", ""),
        opp.get("typeOfSetAsideDescription", ""),
    ]
    return " ".join(p for p in parts if p and isinstance(p, str)).strip()


def _search(api_key: str, params: dict) -> dict:
    for attempt in range(5):
        try:
            resp = httpx.get(
                SEARCH_URL,
                params={"api_key": api_key, **params},
                timeout=httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0),
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.TimeoutException:
            logger.warning(f"SAM timeout (attempt {attempt+1}/5)")
            time.sleep(2 ** attempt)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 65  
                logger.warning(f"SAM rate limited — waiting {wait}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
                continue
            logger.warning(f"SAM HTTP {e.response.status_code} — skipping")
            return {}
        except Exception as e:
            logger.warning(f"SAM request error: {e}")
            time.sleep(1)
    return {}


def fetch_sam(
    days_back:    int        = 90,
    fetch_detail: bool       = False,  
    max_per_keyword: int     = 200,
    keywords:     list[str]  = None,
    max_requests: int        = 800,     
) -> list[dict]:
    api_key     = _get_api_key()
    today       = date.today()
    posted_to   = _date_str(today)
    posted_from = _date_str(today - timedelta(days=days_back))

    kws       = keywords or SEARCH_KEYWORDS
    seen:     set[str]   = set()
    results:  list[dict] = []
    req_count = 0

    logger.info(f"SAM: {len(kws)} keywords | last {days_back} days | "
                f"detail={fetch_detail} | budget={max_requests} req")

    for kw in kws:
        if req_count >= max_requests:
            logger.warning(f"SAM: request budget exhausted ({max_requests}) — stopping.")
            break

        logger.info(f"  '{kw}'...")
        offset   = 0
        kw_added = 0

        while True:
            if req_count >= max_requests:
                break

            data = _search(api_key, {
                "q":          kw,
                "postedFrom": posted_from,
                "postedTo":   posted_to,
                "ptype":      NOTICE_TYPES,
                "active":     "Yes",
                "limit":      PAGE_SIZE,
                "offset":     offset,
            })
            req_count += 1

            if not data:
                break

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
                opp["_agency"]  = _extract_agency(opp)
                opp["_funding"] = _extract_funding(opp)

                if fetch_detail and req_count < max_requests:
                    t = (opp.get("type") or "").lower()
                    if "award" not in t:
                        desc_url = opp.get("description", "")
                        opp["_description_text"] = _fetch_description(desc_url, api_key)
                        if opp["_description_text"]:
                            req_count += 1
                else:
                    opp["_description_text"] = ""

                opp["_full_text"] = _build_full_text(opp)
                results.append(opp)
                kw_added += 1

            offset += PAGE_SIZE
            if offset >= min(total, max_per_keyword):
                break

            time.sleep(PAGE_DELAY)

        logger.info(f"    +{kw_added} new  (total: {len(results)}, req used: {req_count})")

    logger.success(
        f"SAM: {len(results)} unique records | "
        f"{req_count} API requests used (limit: 1,000/day)"
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
    print("\nProbing SAM.gov API...")
    try:
        data  = _search(api_key, {
            "q":          "artificial intelligence",
            "postedFrom": _date_str(today - timedelta(days=30)),
            "postedTo":   _date_str(today),
            "ptype":      NOTICE_TYPES,
            "limit":      1,
        })
        opps  = data.get("opportunitiesData", [])
        total = data.get("totalRecords", 0)
        print(f"  {total} results for 'artificial intelligence' (last 30 days)")
        if opps:
            o = opps[0]
            print(f"  Sample: [{o.get('type')}] {o.get('title','')[:70]}")
            print(f"  Agency: {_extract_agency(o)}")
        return bool(opps)
    except Exception as e:
        print(f"  Error: {e}")
        return False


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    from storage.db import get_db
    from storage.schema import create_schema
    from storage.writer import write_records

    parser = argparse.ArgumentParser()
    parser.add_argument("--probe",     action="store_true")
    parser.add_argument("--disk",      action="store_true")
    parser.add_argument("--detail",    action="store_true", help="Fetch descriptions (uses more quota)")
    parser.add_argument("--days",      type=int, default=90)
    parser.add_argument("--keywords",  nargs="*", default=None)
    args = parser.parse_args()

    if args.probe:
        sys.exit(0 if probe() else 1)

    if args.disk:
        records = load_from_disk()
    else:
        records = fetch_sam(
            days_back    = args.days,
            fetch_detail = args.detail,
            keywords     = args.keywords,
        )
        if records:
            save_raw(records)

    if records:
        con = get_db()
        create_schema(con)
        n = write_records(records, "sam", con)
        logger.success(f"Done — {n} new SAM rows in DB.")