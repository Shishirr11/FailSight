import csv
import hashlib
import json
import re
import time
import argparse
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from loguru import logger

RAW_DIR        = Path(__file__).resolve().parent.parent / "data" / "raw" / "failures"
CBINSIGHTS_DIR = RAW_DIR / "cbinsights"

FAILORY_CEMETERY_URL = "https://www.failory.com/cemetery"

LOOTDROP_BASE_URL      = "https://www.loot-drop.io"
LOOTDROP_SUPABASE_URL  = "https://lentxykytbylpxytluic.supabase.co"
LOOTDROP_ANON_KEY      = "sb_publishable_W5UgIXv8SGHeo43duatMCw_0h8GbgCY"
LOOTDROP_TABLE         = "startups"
LOOTDROP_PAGE_SIZE     = 1000  

LOOTDROP_SELECT = (
    "id,name,description,sector,end_year,total_funding,"
    "difficulty,difficulty_reason,scalability,scalability_reason,"
    "market_potential,market_potential_reason,"
    "created_at,country,start_year,primary_cause_of_death,"
    "product_type,views,condensed_value_prop,condensed_cause_of_death"
)

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REASON_COLUMN_MAP = {
    "Giants":                 "competition_giants",
    "No Budget":              "cash",
    "Competition":            "competition",
    "Poor Market Fit":        "no_pmf",
    "Acquisition Stagnation": "acquisition",
    "Platform Dependency":    "platform_dependency",
    "High Operational Costs": "high_costs",
    "Monetization Failure":   "monetization",
    "Niche Limits":           "niche",
    "Execution Flaws":        "execution",
    "Trend Shifts":           "trend_shift",
    "Toxicity/Trust Issues":  "trust",
    "Regulatory Pressure":    "regulatory",
    "Overhype":               "overhype",
}

CBINSIGHTS_FILE_SECTOR_MAP = {
    "startup failure (finance and insurance).csv": "Fintech",
    "startup failure (food and services).csv":     "Food & Beverage",
    "startup failure (health care).csv":           "Health Technology",
    "startup failure (manufactures).csv":          "Advanced Manufacturing",
    "startup failure (retail trade).csv":          "E-commerce",
    "startup failures (information sector).csv":   "Advanced Computing",
    "startup failures.csv":                        None,
}

_SECTOR_KEYWORDS = {
    "fintech":        "Fintech",         "finance":       "Fintech",
    "crypto":         "Fintech",         "blockchain":    "Fintech",
    "health":         "Health Technology","medical":      "Health Technology",
    "biotech":        "Biotechnology",   "pharma":        "Biotechnology",
    "energy":         "Clean Energy",    "solar":         "Clean Energy",
    "ai":             "AI & Machine Learning","machine learning": "AI & Machine Learning",
    "cybersecurity":  "Cybersecurity",   "security":      "Cybersecurity",
    "edtech":         "Education",       "education":     "Education",
    "agtech":         "Agriculture Technology","agriculture": "Agriculture Technology",
    "saas":           "Advanced Computing","software":    "Advanced Computing",
    "ecommerce":      "E-commerce",      "retail":        "E-commerce",
    "food":           "Food & Beverage", "restaurant":    "Food & Beverage",
    "manufacturing":  "Advanced Manufacturing",
    "space":          "Aerospace & Defense","defense":    "Aerospace & Defense",
    "real estate":    "Real Estate",     "proptech":      "Real Estate",
    "transportation": "Transportation",  "mobility":      "Transportation",
    "media":          "Media",           "entertainment": "Media",
    "gaming":         "Media",           "game":          "Media",
    "logistics":      "Transportation",  "supply chain":  "Transportation",
    "marketplace":    "E-commerce",      "platform":      "Advanced Computing",
    "hr":             "Education",       "talent":        "Education",
    "travel":         "Transportation",  "hospitality":   "Food & Beverage",
    "insurtech":      "Fintech",         "insurance":     "Fintech",
    "legaltech":      "Advanced Computing","legal":       "Advanced Computing",
    "cleantech":      "Clean Energy",    "climate":       "Clean Energy",
}

_LOOTDROP_SECTOR_MAP: dict[str, str] = {
    "health care":            "Health Technology",
    "healthcare":             "Health Technology",
    "information technology": "Advanced Computing",
    "information technology (it)": "Advanced Computing",
    "industrials":            "Advanced Manufacturing",
    "materials":              "Advanced Manufacturing",
    "consumer discretionary": "E-commerce",
    "consumer staples":       "E-commerce",
    "consumer":               "E-commerce",
    "communication services": "Media",
    "communications":         "Media",
    "utilities":              "Clean Energy",
    "energy":                 "Clean Energy",
    "real estate":            "Real Estate",
    "financials":             "Fintech",
    "financial":              "Fintech",
    "technology":             "Advanced Computing",
    "software":               "Advanced Computing",
    "hardware":               "Advanced Computing",
    "saas":                   "Advanced Computing",
    "enterprise software":    "Advanced Computing",
    "developer tools":        "Advanced Computing",
    "infrastructure":         "Advanced Computing",
    "data & analytics":       "Advanced Computing",
    "cybersecurity":          "Cybersecurity",
    "security":               "Cybersecurity",
    "artificial intelligence":"AI & Machine Learning",
    "ai":                     "AI & Machine Learning",
    "machine learning":       "AI & Machine Learning",
    "biotech":                "Biotechnology",
    "biotechnology":          "Biotechnology",
    "life sciences":          "Biotechnology",
    "pharmaceutical":         "Biotechnology",
    "pharma":                 "Biotechnology",
    "medical":                "Health Technology",
    "medtech":                "Health Technology",
    "digital health":         "Health Technology",
    "insurtech":              "Fintech",
    "fintech":                "Fintech",
    "crypto":                 "Fintech",
    "blockchain":             "Fintech",
    "defi":                   "Fintech",
    "web3":                   "Fintech",
    "edtech":                 "Education",
    "education":              "Education",
    "ecommerce":              "E-commerce",
    "e-commerce":             "E-commerce",
    "retail":                 "E-commerce",
    "marketplace":            "E-commerce",
    "d2c":                    "E-commerce",
    "direct to consumer":     "E-commerce",
    "media":                  "Media",
    "entertainment":          "Media",
    "gaming":                 "Media",
    "social media":           "Media",
    "content":                "Media",
    "proptech":               "Real Estate",
    "real estate tech":       "Real Estate",
    "cleantech":              "Clean Energy",
    "climate tech":           "Clean Energy",
    "climate":                "Clean Energy",
    "renewables":             "Clean Energy",
    "agtech":                 "Agriculture Technology",
    "agriculture":            "Agriculture Technology",
    "foodtech":               "Food & Beverage",
    "food & beverage":        "Food & Beverage",
    "food":                   "Food & Beverage",
    "restaurant":             "Food & Beverage",
    "logistics":              "Transportation",
    "transportation":         "Transportation",
    "mobility":               "Transportation",
    "autonomous vehicles":    "Transportation",
    "space":                  "Aerospace & Defense",
    "aerospace":              "Aerospace & Defense",
    "defense":                "Aerospace & Defense",
    "robotics":               "Advanced Manufacturing",
    "manufacturing":          "Advanced Manufacturing",
    "legaltech":              "Advanced Computing",
    "hrtech":                 "Education",
    "hr tech":                "Education",
    "travel":                 "Transportation",
    "hospitality":            "Food & Beverage",
}


def _infer_sector_lootdrop(sector_raw: str, product_type: str, description: str) -> str:
    s = sector_raw.strip().lower()
    if s in _LOOTDROP_SECTOR_MAP:
        return _LOOTDROP_SECTOR_MAP[s]

   
    for key, mapped in _LOOTDROP_SECTOR_MAP.items():
        if key in s:
            return mapped

    
    combined = f"{sector_raw} {product_type} {description[:300]}".lower()
    return _infer_sector(combined)

_LOOTDROP_CAUSE_MAP = {
    "No market need":         "no_pmf",
    "Ran out of cash":        "cash",
    "Not the right team":     "execution",
    "Competition":            "competition",
    "Pricing/cost issues":    "high_costs",
    "Poor product":           "execution",
    "Business model failure": "monetization",
    "Regulatory challenges":  "regulatory",
    "Legal challenges":       "regulatory",
    "Burned out":             "execution",
    "Pivoted":                "trend_shift",
    "Acqui-hired":            "acquisition",
    "Acquired":               "acquisition",
}


def _stable_id(name: str, source: str = "") -> str:
    key = f"{source}:{name.strip().lower()}" if source else name.strip().lower()
    return "fail_" + hashlib.md5(key.encode()).hexdigest()[:16]


def _infer_sector(text: str) -> str:
    t = text.lower()
    for kw, sector in _SECTOR_KEYWORDS.items():
        if kw in t:
            return sector
    return "Other"


def _parse_years(years_str: str) -> tuple[Optional[int], Optional[int]]:
    nums = re.findall(r'\d{4}', str(years_str))
    if len(nums) >= 2:
        return int(nums[0]), int(nums[-1])
    if len(nums) == 1:
        return None, int(nums[0])
    return None, None


def _parse_funding(val: str) -> Optional[float]:
    s = str(val).strip().replace(",", "").replace(" ", "").replace("$", "")
    if not s or s.lower() in ("nodata", "n/a", "unknown", "0", "none", ""):
        return None
    if s == "0":
        return 0.0
    if s.lower() in ("<1m", "<$1m"):
        return 500_000.0
    if s in ("1M-10M", "$1M-$10M"):
        return 5_500_000.0
    if s in ("10M-50M", "$10M-$50M"):
        return 30_000_000.0
    if s.lower() in (">50m", "&gt;$50m", ">$50m"):
        return 50_000_000.0
    s2 = re.sub(r'[^0-9.KMBkmb]', '', s)
    try:
        multiplier = 1
        if s2.upper().endswith("B"):
            multiplier = 1_000_000_000; s2 = s2[:-1]
        elif s2.upper().endswith("M"):
            multiplier = 1_000_000;     s2 = s2[:-1]
        elif s2.upper().endswith("K"):
            multiplier = 1_000;         s2 = s2[:-1]
        return float(s2) * multiplier if s2 else None
    except Exception:
        return None


def _extract_reasons_from_columns(row: dict) -> list[str]:
    reasons = [tag for col, tag in REASON_COLUMN_MAP.items()
               if str(row.get(col, "0")).strip() == "1"]
    return reasons or ["unknown"]


def _extract_reasons_from_text(text: str) -> list[str]:
    t = text.lower()
    reasons = []
    patterns = [
        (["ran out of cash", "no money", "funding dried", "couldn't raise",
          "out of runway", "no more funding", "cash flow"], "cash"),
        (["no market", "no demand", "nobody wanted", "no product-market fit",
          "pmf", "wrong market"], "no_pmf"),
        (["competition", "competitor", "bigger player", "google entered",
          "amazon launched"], "competition"),
        (["team", "co-founder", "founder conflict", "wrong team",
          "talent", "hiring"], "execution"),
        (["regulation", "legal", "compliance", "banned", "fda",
          "government"], "regulatory"),
        (["business model", "monetize", "revenue model", "pricing",
          "couldn't charge"], "monetization"),
        (["pivot", "changed direction", "shifted focus"], "trend_shift"),
        (["acquired", "acqui-hired", "merger"], "acquisition"),
        (["burn rate", "high costs", "expensive", "cost too much",
          "overhead"], "high_costs"),
        (["platform", "api changed", "app store", "google play",
          "dependent on"], "platform_dependency"),
    ]
    for keywords, tag in patterns:
        if any(kw in t for kw in keywords):
            reasons.append(tag)
    return reasons or ["unknown"]


def _parse_one_cbinsights_file(filepath: Path, sector_override: Optional[str]) -> list[dict]:
    results = []
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader  = csv.DictReader(f)
        headers = reader.fieldnames or []
        is_minimal = "What They Did" not in headers

        for row in reader:
            name = (row.get("Name") or "").strip()
            if not name:
                continue

            year_founded, year_failed = _parse_years(row.get("Years of Operation", ""))

            if is_minimal:
                raw_sector = row.get("Sector", "")
                sector     = sector_override or _infer_sector(raw_sector) or "Other"
                results.append({
                    "failure_id":         _stable_id(name, "cbinsights"),
                    "company_name":       name,
                    "sector":             sector,
                    "year_founded":       year_founded,
                    "year_failed":        year_failed,
                    "funding_raised_usd": None,
                    "failure_reasons":    ["unknown"],
                    "stage_at_failure":   "",
                    "key_lesson":         "",
                    "source_url":         "https://www.cbinsights.com/research/startup-failure-post-mortem/",
                    "description":        "",
                    "why_failed":         "",
                    "_source":            "cbinsights",
                    "_file":              filepath.name,
                })
                continue

            funding = _parse_funding(row.get("How Much They Raised", ""))
            reasons = _extract_reasons_from_columns(row)
            desc    = (row.get("What They Did") or "").strip()
            why     = (row.get("Why They Failed") or "").strip()

            if sector_override:
                sector = sector_override
            else:
                sector = _infer_sector(
                    f"{row.get('Sector','')} {desc} {why}"
                ) or "Other"

            results.append({
                "failure_id":         _stable_id(name, "cbinsights"),
                "company_name":       name,
                "sector":             sector,
                "year_founded":       year_founded,
                "year_failed":        year_failed,
                "funding_raised_usd": funding,
                "failure_reasons":    reasons,
                "stage_at_failure":   "",
                "key_lesson":         (row.get("Takeaway") or "").strip(),
                "source_url":         "https://www.cbinsights.com/research/startup-failure-post-mortem/",
                "description":        desc,
                "why_failed":         why,
                "_source":            "cbinsights",
                "_file":              filepath.name,
            })

    logger.debug(f"  {filepath.name}: {len(results)} records")
    return results


def load_cbinsights() -> list[dict]:
    if not CBINSIGHTS_DIR.exists():
        logger.warning(f"CB Insights folder not found at {CBINSIGHTS_DIR}")
        return []
    csv_files = list(CBINSIGHTS_DIR.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {CBINSIGHTS_DIR}")
        return []

    logger.info(f"CB Insights: loading {len(csv_files)} CSV files...")
    all_results = []
    seen: set[str] = set()                                         

    for filepath in sorted(csv_files):
        sector_override = CBINSIGHTS_FILE_SECTOR_MAP.get(filepath.name.lower())
        for record in _parse_one_cbinsights_file(filepath, sector_override):
            fid = record.get("failure_id", "")                     
            if fid and fid not in seen:                            
                seen.add(fid)                                      
                all_results.append(record)                         

    logger.info(f"CB Insights: {len(all_results)} unique records loaded.")
    return all_results


def _scrape_failory_list_page(url: str) -> tuple[list[dict], Optional[str]]:

    logger.info(f"  Failory list: {url}")
    try:
        resp = httpx.get(url, headers=SCRAPE_HEADERS, timeout=30,
                         follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error(f"  List page failed: {e}")
        return [], None

    soup    = BeautifulSoup(resp.text, "html.parser")
    results = []
    cards = soup.find_all("a", class_=lambda c: c and "cemetery-card-link-block" in c)
    for card in cards:
        href = card.get("href", "")
        if not href or href in ("/cemetery", "/cemetery/"):
            continue

        if "homepage" in (card.get("class") or []):
            continue

        def get_field(name):
            el = card.find(attrs={"fs-list-field": name})
            return el.get_text(strip=True) if el else ""

        name = get_field("title")
        if not name:
            continue

        source_url = (
            f"https://www.failory.com{href}"
            if not href.startswith("http") else href
        )
        started     = get_field("started")
        closed      = get_field("closed")
        funding_str = get_field("funding")

        results.append({
            "failure_id":         _stable_id(name, "failory"),
            "company_name":       name,
            "sector":             _infer_sector(
                                      f"{get_field('description')} {get_field('category')}"
                                  ),
            "year_founded":       int(started) if started.isdigit() else None,
            "year_failed":        int(closed)  if closed.isdigit()  else None,
            "funding_raised_usd": _parse_funding(funding_str),
            "failure_reasons":    _extract_reasons_from_text(get_field("failure")),
            "stage_at_failure":   "",
            "key_lesson":         "",
            "source_url":         source_url,
            "description":        get_field("description"),
            "why_failed":         get_field("failure"),
            "outcome":            get_field("outcome"),
            "category":           get_field("category"),
            "country":            get_field("country"),
            "employees":          get_field("employees"),
            "funding_range":      funding_str,
            "_source":            "failory",
            "_detail_scraped":    False,
        })

    next_url = None
    pagination = soup.find("div", class_="w-pagination-wrapper")
    if pagination:
        next_link = pagination.find("a", class_=lambda c: c and "w-pagination-next" in c)
        if next_link:
            next_href = next_link.get("href", "")
            if next_href and next_href != "#":

                base = "https://www.failory.com/cemetery"
                next_url = f"{base}{next_href}" if next_href.startswith("?") else next_href

    return results, next_url


def scrape_failory(deep: bool = True) -> list[dict]:

  #  Pagination format: https://www.failory.com/cemetery?8bd93ea4_page=2
    logger.info("Failory: scraping cemetery list pages...")
    results:  list[dict] = []
    seen_ids: set[str]   = set()
    next_url: Optional[str] = FAILORY_CEMETERY_URL
    page = 1

    while next_url:
        page_results, next_url = _scrape_failory_list_page(next_url)

        new_records = []
        for r in page_results:
            fid = r.get("failure_id", "")
            if fid and fid not in seen_ids:
                seen_ids.add(fid)
                new_records.append(r)

        results.extend(new_records)
        logger.info(f"  Page {page}: {len(new_records)} new records "
                    f"(total so far: {len(results)})"
                    + (f" → next: {next_url}" if next_url else " → last page"))
        page += 1
        time.sleep(1.5)

    logger.info(f"Failory list scrape done: {len(results)} unique entries across {page-1} pages.")

    if deep:
        logger.info("Failory: starting detail page scrape...")
        for i, entry in enumerate(results):
            detail_url = entry.get("source_url", "")
            if not detail_url:
                continue

            detail = _scrape_failory_detail(detail_url)
            if detail:
                if detail.get("founder_names"):
                    entry["founder_names"] = detail["founder_names"]
                if detail.get("full_article"):
                    entry["full_article"] = detail["full_article"]
                    if not entry.get("key_lesson"):
                        paras = [p for p in detail["full_article"].split("\n\n")
                                 if p.strip()]
                        if paras:
                            entry["key_lesson"] = paras[-1][:500]
                    richer_reasons = _extract_reasons_from_text(detail["full_article"])
                    if richer_reasons != ["unknown"]:
                        entry["failure_reasons"] = richer_reasons
                if detail.get("exact_funding_usd") is not None:
                    entry["funding_raised_usd"] = detail["exact_funding_usd"]
                for k in ("num_founders", "num_funding_rounds", "num_investors"):
                    if detail.get(k):
                        entry[k] = detail[k]
                if detail.get("num_employees"):
                    entry["num_employees"] = detail["num_employees"]
                if detail.get("country"):
                    entry["country"] = detail["country"]
                if detail.get("outcome"):
                    entry["outcome"] = detail["outcome"]
                if detail.get("category"):
                    entry["category"] = detail["category"]
                entry["_detail_scraped"] = True

            time.sleep(1.0)
            if (i + 1) % 20 == 0:
                logger.info(f"  Detail scraped {i + 1}/{len(results)}...")

        logger.info("Failory detail scrape complete.")

    return results


def _scrape_failory_detail(url: str) -> dict:
    try:
        resp = httpx.get(url, headers=SCRAPE_HEADERS, timeout=30,
                         follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.debug(f"  Detail fetch failed {url}: {e}")
        return {}

    soup   = BeautifulSoup(resp.text, "html.parser")
    result = {}
    data_card = soup.find("div", class_="cemetery-page-data-card")
    if data_card:
        def get_data_field(label: str) -> str:
            """Find a row by its category label and return the information value."""
            for cat in data_card.find_all("div", class_="cemetery-page-data-category"):
                if label.lower() in cat.get_text(strip=True).lower():
                    info = cat.find_next_sibling("div", class_="cemetery-page-data-information")
                    if info:
                        return info.get_text(strip=True)
            return ""

       
        founders = get_data_field("Name of Founders")
        if founders:
            result["founder_names"] = founders

        num_founders = get_data_field("Number of Founders")
        if num_founders and num_founders.isdigit():
            result["num_founders"] = int(num_founders)

        num_employees = get_data_field("Number of Employees")
        if num_employees:
            result["num_employees"] = num_employees

        num_rounds = get_data_field("Number of Funding Rounds")
        if num_rounds and num_rounds.isdigit():
            result["num_funding_rounds"] = int(num_rounds)

        num_investors = get_data_field("Number of Investors")
        if num_investors and num_investors.isdigit():
            result["num_investors"] = int(num_investors)
       
        funding_str = get_data_field("Total Funding Amount")
        if funding_str:
            result["exact_funding_usd"] = _parse_funding(funding_str)
            result["funding_amount_str"] = funding_str 

        outcome = get_data_field("Outcome")
        if outcome:
            result["outcome"] = outcome

        cause = get_data_field("Cause")
        if cause:
            result["cause"] = cause

        country = get_data_field("Country")
        if country:
            result["country"] = country

        category = get_data_field("Category")
        if category:
            result["category"] = category
    article_block = soup.find("div", class_="div-block-cemetery-article")

    
    if article_block and "w-condition-invisible" in (article_block.get("class") or []):
        for sibling in article_block.find_next_siblings("div"):
            if "div-block-cemetery-article" in (sibling.get("class") or []):
                if "w-condition-invisible" not in (sibling.get("class") or []):
                    article_block = sibling
                    break

    if article_block:
        rich_text = article_block.find("div", class_=lambda c: c and "w-richtext" in c)
        content_el = rich_text or article_block
    else:
        content_el = soup.find(
            "div",
            class_=lambda c: c and "content-black-rich-text" in c
                              and "w-richtext" in c
        )

    if content_el:
        paragraphs = []
        for el in content_el.find_all(["p", "h2", "h3", "h4", "li", "blockquote"]):
            text = el.get_text(separator=" ", strip=True)
            if text and len(text) > 20:
                if text.startswith("{") or text.startswith("@context"):
                    continue
                paragraphs.append(text)

        if paragraphs:
            result["full_article"] = "\n\n".join(paragraphs)

    return result


def _lootdrop_supabase_fetch() -> list[dict]:
    headers = {
        "apikey":        LOOTDROP_ANON_KEY,
        "Authorization": f"Bearer {LOOTDROP_ANON_KEY}",
        "Accept":        "application/json",
    }

    all_records: list[dict] = []
    offset = 0

    while True:
        url = (
            f"{LOOTDROP_SUPABASE_URL}/rest/v1/{LOOTDROP_TABLE}"
            f"?select={LOOTDROP_SELECT}"
            f"&order=id.desc"
            f"&offset={offset}"
            f"&limit={LOOTDROP_PAGE_SIZE}"
        )
        try:
            resp = httpx.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            batch = resp.json()
        except httpx.HTTPStatusError as e:
            logger.error(
                f"LootDrop Supabase HTTP {e.response.status_code} "
                f"at offset {offset}: {e.response.text[:200]}"
            )
            break
        except Exception as e:
            logger.error(f"LootDrop Supabase fetch error at offset {offset}: {e}")
            break

        if not batch:
            break

        all_records.extend(batch)
        logger.info(
            f"  LootDrop: fetched {len(all_records)} records "
            f"(batch={len(batch)})..."
        )

        if len(batch) < LOOTDROP_PAGE_SIZE:
            break  

        offset += LOOTDROP_PAGE_SIZE
        time.sleep(0.2)

    return all_records


def _normalize_lootdrop_record(raw: dict) -> dict:

    name = str(raw.get("name") or "").strip()
    if not name:
        return {}

    try:
        year_founded = int(raw["start_year"]) if raw.get("start_year") else None
    except (ValueError, TypeError):
        year_founded = None
    try:
        year_failed = int(raw["end_year"]) if raw.get("end_year") else None
    except (ValueError, TypeError):
        year_failed = None

    funding_raw = raw.get("total_funding")
    try:
        funding_usd = float(funding_raw) if funding_raw not in (None, "", "0") else None
    except (ValueError, TypeError):
        funding_usd = _parse_funding(str(funding_raw)) if funding_raw else None

    sector = _infer_sector_lootdrop(
        sector_raw   = str(raw.get("sector", "")),
        product_type = str(raw.get("product_type", "")),
        description  = str(raw.get("description", "")),
    )

    cause_primary    = str(raw.get("primary_cause_of_death") or "")
    cause_condensed  = str(raw.get("condensed_cause_of_death") or "")
    cause_combined   = f"{cause_primary} {cause_condensed}"
    mapped_tag       = _LOOTDROP_CAUSE_MAP.get(cause_primary.strip())
    if mapped_tag:
        reasons = [mapped_tag]
    elif cause_combined.strip():
        reasons = _extract_reasons_from_text(cause_combined)
    else:
        reasons = ["unknown"]

    description   = str(raw.get("description") or "").strip()
    value_prop    = str(raw.get("condensed_value_prop") or "").strip()
    why_failed    = str(raw.get("condensed_cause_of_death") or "").strip()
    diff_reason   = str(raw.get("difficulty_reason") or "").strip()
    scale_reason  = str(raw.get("scalability_reason") or "").strip()
    market_reason = str(raw.get("market_potential_reason") or "").strip()

    full_desc = "\n\n".join(filter(None, [description, value_prop]))
    why_failed_full = "\n\n".join(filter(None, [why_failed, diff_reason]))
    key_lesson = scale_reason or diff_reason or market_reason
    country = str(raw.get("country") or "").strip()

    record_id  = raw.get("id", "")
    source_url = (
        f"https://www.loot-drop.io/startup/{record_id}"
        if record_id else LOOTDROP_BASE_URL
    )

    scores = {
        "difficulty":              raw.get("difficulty"),
        "difficulty_reason":       diff_reason,
        "scalability":             raw.get("scalability"),
        "scalability_reason":      scale_reason,
        "market_potential":        raw.get("market_potential"),
        "market_potential_reason": market_reason,
        "views":                   raw.get("views"),
        "product_type":            raw.get("product_type", ""),
        "sector_raw":              raw.get("sector", ""),
    }

    return {
        "failure_id":         _stable_id(name, "lootdrop"),
        "company_name":       name,
        "sector":             sector,
        "year_founded":       year_founded,
        "year_failed":        year_failed,
        "funding_raised_usd": funding_usd,
        "failure_reasons":    reasons,
        "stage_at_failure":   "",
        "key_lesson":         key_lesson[:1000],
        "source_url":         source_url,
        "description":        full_desc[:3000],
        "why_failed":         why_failed_full[:2000],
        "country":            country,
        "outcome":            cause_primary[:200],
        "funding_range":      str(funding_raw or ""),
        "_source":            "lootdrop",
        "_lootdrop_id":       str(record_id),
        "_scores":            json.dumps(scores, default=str),
    }


def _fetch_lootdrop_detail(record_id: str) -> dict:
    url = f"{LOOTDROP_BASE_URL}/startup/{record_id}"
    try:
        resp = httpx.get(url, headers=SCRAPE_HEADERS, timeout=25,
                         follow_redirects=True)
        if resp.status_code != 200:
            return {}

        soup = BeautifulSoup(resp.text, "html.parser")

        result: dict = {}

        card_map = {}
        for card in soup.find_all("article", class_="grid-card"):
            title_el = card.find(class_="card-title")
            title    = title_el.get_text(strip=True) if title_el else ""
            full_text = card.get("data-full-text", "").strip()
            if not full_text:
                text_el   = card.find(class_="card-text")
                full_text = text_el.get_text(strip=True) if text_el else ""
            if title and full_text:
                card_map[title.lower()] = full_text

        CARD_FIELD_MAP = {
            "failure analysis":  "failure_analysis",
            "market analysis":   "market_analysis",
            "startup learnings": "startup_learnings",
            "market potential":  "market_potential_full",
            "difficulty":        "difficulty_full",
            "scalability":       "scalability_full",
        }
        for card_title, field_name in CARD_FIELD_MAP.items():
            for key, text in card_map.items():
                if card_title in key:
                    result[field_name] = text
                    break

        hero_story = soup.find(class_="hero-story")
        if hero_story:
            result["hero_description"] = hero_story.get_text(strip=True)

        pivot_el = soup.find(class_="section-text-large")
        if pivot_el:
            result["rebuild_concept"] = pivot_el.get_text(strip=True)

        sections = []
        for field in ("failure_analysis", "market_analysis", "startup_learnings",
                      "market_potential_full", "difficulty_full", "scalability_full"):
            text = result.get(field, "")
            if text:
                label = field.replace("_full", "").replace("_", " ").title()
                sections.append(f"[{label}]\n{text}")

        if sections:
            result["full_article"] = "\n\n".join(sections)

        return result

    except Exception as e:
        logger.debug(f"  LootDrop detail fetch error {url}: {e}")
        return {}


def scrape_lootdrop() -> list[dict]:

    logger.info("LootDrop: fetching from Supabase REST API...")
    raw_records = _lootdrop_supabase_fetch()

    if not raw_records:
        logger.error("LootDrop: 0 records returned from Supabase API.")
        return []

    logger.info(f"LootDrop: normalizing {len(raw_records)} raw records...")
    results = []
    for raw in raw_records:
        normalized = _normalize_lootdrop_record(raw)
        if normalized.get("company_name"):
            results.append(normalized)

    logger.success(f"LootDrop: {len(results)} valid records normalized.")

    logger.info("LootDrop: fetching detail pages for full card content...")
    for i, entry in enumerate(results):
        lootdrop_id = entry.get("_lootdrop_id", "")
        if not lootdrop_id:
            continue
        detail = _fetch_lootdrop_detail(lootdrop_id)
        if not detail:
            time.sleep(0.8)
            continue
        if detail.get("full_article"):
            entry["full_article"] = detail["full_article"]

        if detail.get("hero_description"):
            entry["description"] = detail["hero_description"]

        if detail.get("failure_analysis"):
            entry["why_failed"] = detail["failure_analysis"]

        if detail.get("startup_learnings"):
            entry["key_lesson"] = detail["startup_learnings"][:2000]

        for field in ("market_analysis", "market_potential_full",
                        "difficulty_full", "scalability_full", "rebuild_concept"):
            if detail.get(field):
                entry[f"_detail_{field}"] = detail[field]

        if detail.get("failure_analysis"):
            richer = _extract_reasons_from_text(detail["failure_analysis"])
            if richer != ["unknown"]:
                entry["failure_reasons"] = richer
        time.sleep(1.0)  
        if (i + 1) % 50 == 0:
            logger.info(f"  LootDrop detail: {i+1}/{len(results)}")

    return results


def fetch_failures(
    include_failory:  bool = True,
    include_lootdrop: bool = True,
    deep:             bool = True,
) -> list[dict]:

    seen:    set[str]   = set()
    results: list[dict] = []

    all_records: list[dict] = []
    all_records += load_cbinsights()
    if include_failory:
        all_records += scrape_failory(deep=deep)
    if include_lootdrop:
        all_records += scrape_lootdrop()

    for record in all_records:
        fid = record.get("failure_id", "")
        name_key = record.get("company_name", "").strip().lower()
        dedup_key = fid or name_key
        if dedup_key and dedup_key not in seen:
            seen.add(dedup_key)
            if name_key:
                seen.add(name_key)
            results.append(record)

    by_source = Counter(r.get("_source", "unknown") for r in results)
    logger.success(f"Failures: {len(results)} unique records total")
    for src, count in by_source.items():
        logger.info(f"  {src:<15} {count}")
    return results


def save_raw(data: list[dict]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out = RAW_DIR / f"{date.today()}.json"
    out.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"Raw failures saved → {out} ({len(data)} records)")
    return out


def load_from_disk() -> list[dict]:
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No raw failures files in {RAW_DIR}")
    logger.info(f"Loading from disk: {files[-1].name}")
    return json.loads(files[-1].read_text())


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    from storage.db import get_db
    from storage.schema import create_schema
    from storage.writer import write_records

    parser = argparse.ArgumentParser(description="Failure data collector")
    parser.add_argument("--disk",           action="store_true",
                        help="Load from latest saved file instead of live fetch")
    parser.add_argument("--no-failory",     action="store_true",
                        help="Skip Failory scrape")
    parser.add_argument("--no-lootdrop",    action="store_true",
                        help="Skip LootDrop fetch")
    parser.add_argument("--no-deep",        action="store_true",
                        help="Skip Failory detail page scrape")
    parser.add_argument("--source",
                        choices=["cbinsights", "failory", "lootdrop"],
                        help="Fetch only this source")
    args = parser.parse_args()

    if args.disk:
        records = load_from_disk()
    elif args.source == "lootdrop":
        records = scrape_lootdrop()
        save_raw(records)
    elif args.source == "failory":
        records = scrape_failory(deep=not args.no_deep)
        save_raw(records)
    elif args.source == "cbinsights":
        records = load_cbinsights()
        save_raw(records)
    else:
        records = fetch_failures(
            include_failory  = not args.no_failory,
            include_lootdrop = not args.no_lootdrop,
            deep             = not args.no_deep,
        )
        save_raw(records)

    con = get_db()
    create_schema(con)
    n = write_records(records, "failures", con)
    logger.success(f"Done — {n} new failure rows in DB.")


"""
    python -m collectors.failure_collector --no-deep                 
    python -m collectors.failure_collector --source lootdrop  
"""
