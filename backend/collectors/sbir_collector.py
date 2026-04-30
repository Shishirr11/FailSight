import csv
import json
import time
import hashlib
from datetime import date
from pathlib import Path
from typing import Optional

import httpx
from loguru import logger

BASE_URL  = "https://api.www.sbir.gov/public/api/awards"
RAW_DIR   = Path(__file__).resolve().parent.parent / "data" / "raw" / "sbir"
PAGE_SIZE = 100
PAGE_DELAY = 0.3

AGENCIES = ["DOD", "HHS", "NASA", "NSF", "DOE", "USDA", "EPA", "DOC", "ED", "DOT", "DHS"]

MIN_YEAR = 2018

SECTOR_KEYWORDS: dict[str, list[str]] = {
    "AI & Machine Learning":    ["artificial intelligence", "machine learning", "deep learning",
                                  "neural network", "natural language", "computer vision",
                                  "large language model", "reinforcement learning", "llm"],
    "Cybersecurity":            ["cybersecurity", "cyber security", "network security",
                                  "zero trust", "encryption", "intrusion detection", "malware"],
    "Clean Energy":             ["renewable energy", "solar", "wind energy", "energy storage",
                                  "battery", "hydrogen", "fuel cell", "smart grid"],
    "Climate Technology":       ["carbon capture", "climate change", "carbon dioxide",
                                  "greenhouse gas", "sustainability", "emissions reduction"],
    "Biotechnology":            ["biotechnology", "genomics", "crispr", "drug discovery",
                                  "protein", "synthetic biology", "gene therapy"],
    "Health Technology":        ["medical device", "telehealth", "clinical", "diagnostic",
                                  "therapeutics", "health monitoring", "wearable health"],
    "Quantum Computing":        ["quantum computing", "quantum information", "qubit",
                                  "quantum sensing", "quantum communication"],
    "Advanced Manufacturing":   ["additive manufacturing", "3d printing", "robotics",
                                  "advanced manufacturing", "industrial automation"],
    "Aerospace & Defense":      ["autonomous system", "unmanned", "drone", "satellite",
                                  "hypersonic", "propulsion", "aerospace"],
    "Advanced Computing":       ["semiconductor", "microelectronics", "chip design",
                                  "edge computing", "photonics", "neuromorphic"],
    "Agriculture Technology":   ["precision agriculture", "food safety", "agtech",
                                  "crop monitoring", "soil health"],
}


def _stable_id(row: dict) -> str:
    contract = row.get("Contract") or row.get("Agency Tracking Number") or ""
    if contract.strip():
        return "sbir_" + hashlib.md5(contract.strip().encode()).hexdigest()[:16]
    raw = f"{row.get('Company','')}{row.get('Award Title','')}{row.get('Award Year','')}"
    return "sbir_" + hashlib.md5(raw.encode()).hexdigest()[:16]


def _infer_sector(text: str) -> str:
    t = text.lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in t for kw in keywords):
            return sector
    return "Other"


def _is_relevant(row: dict) -> bool:
    year = row.get("Award Year", "")
    if year:
        try:
            if int(str(year).strip()) < MIN_YEAR:
                return False
        except (ValueError, TypeError):
            pass

    text = " ".join(filter(None, [
        row.get("Award Title", ""),
        row.get("Abstract", ""),
    ])).lower()

    if not text.strip():
        return False

    for keywords in SECTOR_KEYWORDS.values():
        if any(kw in text for kw in keywords):
            return True
    return False


def _safe_float(v) -> Optional[float]:
    if not v:
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def load_bulk(path: Path = None) -> list[dict]:
    if path is None:
        candidates = (
            list(RAW_DIR.glob("awards_full.csv")) +
            sorted(RAW_DIR.glob("*.csv"))
        )
        if not candidates:
            raise FileNotFoundError(
                f"No SBIR CSV file found in {RAW_DIR}\n"
                f"Download from: https://www.sbir.gov/data-resources\n"
                f"Save as: {RAW_DIR}/awards_full.csv"
            )
        path = candidates[0]

    logger.info(f"Loading SBIR CSV: {path.name} ({path.stat().st_size / 1e6:.0f} MB)")

    results = []
    total   = 0

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            if _is_relevant(row):
                text = f"{row.get('Award Title','')} {row.get('Abstract','')}"
                row["_foip_id"] = _stable_id(row)
                row["_sector"]  = _infer_sector(text)
                row["_keyword"] = row.get("Award Title", "")[:80]
                results.append(dict(row))

    logger.success(
        f"SBIR CSV: {total:,} total rows → "
        f"{len(results):,} tech-relevant (year ≥ {MIN_YEAR})"
    )
    return results



def _api_fetch(params: dict) -> list[dict]:
    try:
        resp = httpx.get(
            BASE_URL, params=params,
            timeout=httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0),
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else (data.get("awards") or [])
    except Exception as e:
        logger.warning(f"SBIR API error: {e}")
        return []


def fetch_sbir_api(max_per_agency_year: int = 500) -> list[dict]:
    current_year = date.today().year
    years = list(range(current_year - 3, current_year + 1))

    seen:    set[str]   = set()
    results: list[dict] = []

    logger.info(f"SBIR API: {len(AGENCIES)} agencies × {len(years)} years")

    for agency in AGENCIES:
        for year in years:
            logger.info(f"  {agency} {year}...")
            start = 0

            while True:
                awards = _api_fetch({
                    "agency": agency,
                    "year":   year,
                    "rows":   PAGE_SIZE,
                    "start":  start,
                })
                if not awards:
                    break

                for award in awards:
                    row = {
                        "Company":                award.get("firm", ""),
                        "Award Title":            award.get("award_title", ""),
                        "Agency":                 award.get("agency", ""),
                        "Branch":                 award.get("branch", ""),
                        "Phase":                  award.get("phase", ""),
                        "Award Year":             award.get("award_year", ""),
                        "Award Amount":           award.get("award_amount", ""),
                        "Abstract":               award.get("abstract", ""),
                        "Contact Name":           award.get("poc_name", ""),
                        "Contact Email":          award.get("poc_email", ""),
                        "PI Name":                award.get("pi_name", ""),
                        "State":                  award.get("state", ""),
                        "Contract":               award.get("contract", ""),
                        "Agency Tracking Number": award.get("agency_tracking_number", ""),
                        "Solicitation Number":    award.get("solicitation_number", ""),
                        "Company Website":        award.get("company_url", ""),
                    }
                    aid = _stable_id(row)
                    if aid in seen or not _is_relevant(row):
                        continue
                    seen.add(aid)
                    text = f"{row['Award Title']} {row['Abstract']}"
                    row["_foip_id"] = aid
                    row["_sector"]  = _infer_sector(text)
                    row["_keyword"] = row["Award Title"][:80]
                    results.append(row)

                start += PAGE_SIZE
                if len(awards) < PAGE_SIZE or start >= max_per_agency_year:
                    break
                time.sleep(PAGE_DELAY)

    logger.success(f"SBIR API: {len(results)} relevant awards")
    return results


def fetch_sbir(**kwargs) -> list[dict]:
    return fetch_sbir_api(**kwargs)


def save_raw(data: list[dict]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out = RAW_DIR / f"{date.today()}_filtered.json"
    out.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"Filtered SBIR saved → {out} ({len(data)} records)")
    return out


def load_from_disk() -> list[dict]:
    dated = sorted(RAW_DIR.glob("*_filtered.json"))
    if dated:
        logger.info(f"Loading filtered cache: {dated[-1].name}")
        return json.loads(dated[-1].read_text())
    return load_bulk()


def probe() -> bool:
    print("\nChecking SBIR data availability...")

    csv_file = RAW_DIR / "awards_full.csv"
    if csv_file.exists():
        size_mb = csv_file.stat().st_size / 1e6
        print(f"   Bulk CSV found: {csv_file.name} ({size_mb:.0f} MB)")
        print(f"  Run: python3 scripts/ingest.py --source sbir --disk")
        return True

    any_csv = list(RAW_DIR.glob("*.csv"))
    if any_csv:
        print(f"   CSV found: {any_csv[0].name}")
        print(f"  Run: python3 scripts/ingest.py --source sbir --disk")
        return True

    print(f"   No CSV found in {RAW_DIR}")
    print(f"  Download from: https://www.sbir.gov/data-resources")
    print(f"  File: 'Award data with abstract information' (CSV)")
    print(f"  Save as: {RAW_DIR}/awards_full.csv")

    print(f"\n  Testing live API...")
    awards = _api_fetch({"agency": "DOD", "rows": 1})
    if awards:
        print(f"   API is up — {awards[0].get('award_title','')[:60]}")
        return True
    else:
        print(f"   API is also down (under maintenance)")
        return False


if __name__ == "__main__":
    import sys, argparse
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from storage.db import get_db
    from storage.schema import create_schema
    from storage.writer import write_records

    parser = argparse.ArgumentParser()
    parser.add_argument("--probe",     action="store_true")
    parser.add_argument("--disk",      action="store_true")
    parser.add_argument("--csv-file",  type=str, default=None)
    args = parser.parse_args()

    if args.probe:
        sys.exit(0 if probe() else 1)

    if args.disk or args.csv_file:
        records = load_bulk(Path(args.csv_file) if args.csv_file else None)
    else:
        records = fetch_sbir_api()

    if records:
        save_raw(records)
        con = get_db()
        create_schema(con)
        n = write_records(records, "sbir", con)
        logger.success(f"Done — {n} new SBIR rows in DB.")
