import json
import time
import hashlib
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import httpx
from loguru import logger

BASE_URL  = "http://api.nsf.gov/services/v1/awards.json"
RAW_DIR   = Path(__file__).resolve().parent.parent / "data" / "raw" / "nsf"
PAGE_SIZE = 25    
PAGE_DELAY = 0.25
MIN_YEAR   = 2020

SEARCH_TOPICS: list[tuple[str, str]] = [
    ("artificial intelligence",     "AI & Machine Learning"),
    ("machine learning",            "AI & Machine Learning"),
    ("deep learning",               "AI & Machine Learning"),
    ("natural language processing", "AI & Machine Learning"),
    ("computer vision",             "AI & Machine Learning"),
    ("large language model",        "AI & Machine Learning"),
    ("cybersecurity",               "Cybersecurity"),
    ("network security",            "Cybersecurity"),
    ("cryptography",                "Cybersecurity"),
    ("renewable energy",            "Clean Energy"),
    ("energy storage",              "Clean Energy"),
    ("solar cell",                  "Clean Energy"),
    ("carbon capture",              "Climate Technology"),
    ("climate change",              "Climate Technology"),
    ("biotechnology",               "Biotechnology"),
    ("genomics",                    "Biotechnology"),
    ("CRISPR",                      "Biotechnology"),
    ("quantum computing",           "Quantum Computing"),
    ("quantum information",         "Quantum Computing"),
    ("robotics",                    "Advanced Manufacturing"),
    ("additive manufacturing",      "Advanced Manufacturing"),
    ("semiconductor",               "Advanced Computing"),
    ("microelectronics",            "Advanced Computing"),
    ("precision agriculture",       "Agriculture Technology"),
    ("drug discovery",              "Biotechnology"),
    ("autonomous vehicle",          "Aerospace & Defense"),
    ("unmanned systems",            "Aerospace & Defense"),
    ("telehealth digital health",      "Health Technology"),
    ("medical device wearable",        "Health Technology"),
    ("autonomous drone UAV",           "Aerospace & Defense"),
    ("satellite remote sensing",       "Aerospace & Defense"),
    ("hypersonic vehicle",             "Aerospace & Defense"),
    ("financial technology blockchain", "Fintech"),
    ("decentralized finance digital payment", "Fintech")
]

PRINT_FIELDS = ",".join([
    "id", "title", "agency", "awardeeName", "awardeeCity", "awardeeStateCode",
    "piFirstName", "piLastName", "piEmail",
    "abstractText",
    "fundsObligated", "estimatedTotalAmt",
    "startDate", "expDate",
    "primaryProgram", "fundProgramName", "cfdaNumber", "transType",
    "orgLongName", "perfStateCode",
])



def _stable_id(award: dict) -> str:
    nsf_id = award.get("id", "")
    if nsf_id:
        return f"nsf_{nsf_id}"
    raw = f"{award.get('title','')}{award.get('awardeeName','')}"
    return "nsf_" + hashlib.md5(raw.encode()).hexdigest()[:16]


def _safe_float(v) -> Optional[float]:
    if not v:
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def _search(keyword: str, offset: int = 0, date_start: str = None) -> list[dict]:
    params = {
        "keyword":     keyword,
        "offset":      offset,
        "rpp":         PAGE_SIZE,
        "printFields": PRINT_FIELDS,
    }
    if date_start:
        params["dateStart"] = date_start

    try:
        resp = httpx.get(
            BASE_URL,
            params=params,
            timeout=httpx.Timeout(connect=10.0, read=25.0, write=10.0, pool=10.0),
            follow_redirects=True,
        )
        resp.raise_for_status()
        data   = resp.json()
        awards = (data.get("response", {}) or {}).get("award", []) or []
        return awards
    except httpx.TimeoutException:
        logger.warning(f"NSF timeout for '{keyword}' offset={offset}")
        return []
    except Exception as e:
        logger.warning(f"NSF error for '{keyword}': {e}")
        return []


def fetch_nsf(
    topics:        list[tuple[str, str]] = None,
    max_per_topic: int = 200,
    years_back:    int = 5,
) -> list[dict]:
    topics     = topics or SEARCH_TOPICS
    date_start = (date.today() - timedelta(days=365 * years_back)).strftime("%m/%d/%Y")
    seen:    set[str]   = set()
    results: list[dict] = []

    logger.info(f"NSF: {len(topics)} keywords | since {date_start} | max {max_per_topic}/keyword")

    for keyword, sector in topics:
        logger.info(f"  '{keyword}'...")
        offset = 0
        added  = 0

        while True:
            awards = _search(keyword, offset, date_start)
            if not awards:
                break

            for award in awards:
                aid = _stable_id(award)
                if aid in seen:
                    continue
                seen.add(aid)
                award["_foip_id"] = aid
                award["_sector"]  = sector
                award["_keyword"] = keyword
                results.append(award)
                added += 1

            offset += PAGE_SIZE
            if len(awards) < PAGE_SIZE or offset >= max_per_topic:
                break
            time.sleep(PAGE_DELAY)

        logger.debug(f"    +{added} (total: {len(results)})")

    logger.success(f"NSF: {len(results)} unique awards fetched")
    return results


def save_raw(data: list[dict]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out = RAW_DIR / f"{date.today()}.json"
    out.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"Raw NSF saved → {out} ({len(data)} records)")
    return out


def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No raw NSF files in {RAW_DIR}")
    logger.info(f"Loading from disk: {files[-1].name}")
    return json.loads(files[-1].read_text())


def probe() -> bool:
    print("\nProbing NSF Awards API...")
    awards = _search("artificial intelligence", 0)
    print(f"  {len(awards)} results returned (page 1)")
    if awards:
        a = awards[0]
        amt = a.get("fundsObligatedAmt") or a.get("estimatedTotalAmt") or "?"
        print(f"  Sample: {a.get('awardeeName','?')[:50]}")
        print(f"  Title:  {a.get('title','')[:70]}")
        print(f"  Amount: ${amt}  PI: {a.get('piFirstName','')} {a.get('piLastName','')}")
        print(f"  Fields: {list(a.keys())}")
        return True
    print("  No results — API may be down or URL wrong")
    return False


if __name__ == "__main__":
    import sys, argparse
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from storage.db import get_db
    from storage.schema import create_schema
    from storage.writer import write_records

    parser = argparse.ArgumentParser()
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--disk",  action="store_true")
    parser.add_argument("--max",   type=int, default=200)
    parser.add_argument("--years", type=int, default=5)
    args = parser.parse_args()

    if args.probe:
        sys.exit(0 if probe() else 1)

    records = load_from_disk() if args.disk else fetch_nsf(
        max_per_topic=args.max, years_back=args.years
    )
    if records:
        save_raw(records)
        con = get_db()
        create_schema(con)
        n = write_records(records, "nsf", con)
        logger.success(f"Done — {n} new NSF rows in DB.")

"""
API docs: http://www.research.gov/common/webapi/awardapisearch-v1.htm
Base URL: http://api.nsf.gov/services/v1/awards.json
"""