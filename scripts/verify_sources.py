import sys
import os
import json
import argparse
import time
from pathlib import Path
from datetime import date, timedelta
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import httpx
from loguru import logger
from storage.db import get_db
from storage.schema import create_schema

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}[PASS]{RESET} {msg}")
def fail(msg): print(f"  {RED}[FAIL]{RESET} {msg}"); _failures.append(msg)
def warn(msg): print(f"  {YELLOW}[WARN]{RESET} {msg}")
def info(msg): print(f"  {CYAN}[INFO]{RESET} {msg}")

_failures: list[str] = []

def _db_counts(con, source: str) -> dict:
    if source == "failures":
        total = con.execute("SELECT COUNT(*) FROM failures_unified").fetchone()[0]
        by_src = con.execute("""
            SELECT CAST(raw_json->>'_source' AS VARCHAR) as src, COUNT(*) as n
            FROM failures_unified GROUP BY src ORDER BY n DESC
        """).fetchall()
        enr = con.execute("""
            SELECT COUNT(*) FROM enriched_details
            WHERE source IN ('cbinsights','failory') AND enrichment_status = 'done'
        """).fetchone()[0]
        return {"total": total, "by_sub_source": by_src, "enriched": enr}
    else:
        total = con.execute(
            "SELECT COUNT(*) FROM unified_opportunities WHERE source = ?", [source]
        ).fetchone()[0]
        enr = con.execute("""
            SELECT COUNT(*) FROM enriched_details
            WHERE source = ? AND enrichment_status = 'done'
        """, [source]).fetchone()[0]
        return {"total": total, "enriched": enr}

def _field_completeness(con, source: str) -> dict:
    if source == "failures":
        table = "failures_unified"
        fields = ["company_name", "sector", "year_failed",
                  "funding_raised_usd", "key_lesson", "failure_reasons"]
        total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    else:
        table = "unified_opportunities"
        fields = ["title", "description", "sector",
                  "posted_date", "close_date", "funding_max", "agency"]
        total = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE source = ?", [source]
        ).fetchone()[0]

    if total == 0:
        return {}

    completeness = {}
    for field in fields:
        if source == "failures":
            q = f"""
                SELECT COUNT(*) FROM {table}
                WHERE {field} IS NOT NULL
                  AND CAST({field} AS VARCHAR) NOT IN ('', '[]', 'null', '["unknown"]')
            """
        else:
            q = f"""
                SELECT COUNT(*) FROM {table}
                WHERE source = ? AND {field} IS NOT NULL
                  AND CAST({field} AS VARCHAR) NOT IN ('', 'null')
            """
        if source == "failures":
            count = con.execute(q).fetchone()[0]
        else:
            count = con.execute(q, [source]).fetchone()[0]
        completeness[field] = round(count / total * 100, 1)
    return completeness

def _enrichment_quality(con, source: str) -> dict:
    if source == "failures":
        src_filter = "source IN ('cbinsights','failory','lootdrop')"
        params = []
    else:
        src_filter = "source = ?"
        params = [source]

    query = f"""
        SELECT
            COUNT(*)                                         AS total,
            COUNT(*) FILTER (WHERE full_text IS NOT NULL
                              AND LENGTH(full_text) > 50)   AS has_full_text,
            COUNT(*) FILTER (WHERE summary IS NOT NULL
                              AND LENGTH(summary) > 20)     AS has_summary,
            AVG(LENGTH(full_text))  FILTER (WHERE full_text IS NOT NULL) AS avg_ft_len,
            MIN(LENGTH(full_text))  FILTER (WHERE full_text IS NOT NULL) AS min_ft_len,
            MAX(LENGTH(full_text))  FILTER (WHERE full_text IS NOT NULL) AS max_ft_len
        FROM enriched_details
        WHERE {src_filter} AND enrichment_status = 'done'
    """
    row = con.execute(query, params).fetchone() if params else con.execute(query).fetchone()
    if not row or row[0] == 0:
        return {}
    return {
        "total":         int(row[0]),
        "has_full_text": int(row[1]),
        "has_summary":   int(row[2]),
        "avg_ft_len":    int(row[3] or 0),
        "min_ft_len":    int(row[4] or 0),
        "max_ft_len":    int(row[5] or 0),
    }

def _sample_records(con, source: str, n: int = 2) -> list[dict]:

    if source == "failures":
        rows = con.execute(f"""
            SELECT
                failure_id, company_name, sector, year_failed,
                funding_raised_usd, failure_reasons, key_lesson,
                CAST(raw_json->>'_source' AS VARCHAR) AS _source
            FROM failures_unified
            WHERE key_lesson IS NOT NULL AND key_lesson != ''
            ORDER BY RANDOM() LIMIT {n}
        """).fetchdf().to_dict(orient="records")
    else:
        rows = con.execute(f"""
            SELECT
                opp_id, title, sector, posted_date::VARCHAR AS posted_date,
                close_date::VARCHAR AS close_date,
                funding_max, agency, description
            FROM unified_opportunities
            WHERE source = ?
              AND title IS NOT NULL AND title != ''
            ORDER BY RANDOM() LIMIT {n}
        """, [source]).fetchdf().to_dict(orient="records")
    return rows

def _sector_distribution(con, source: str) -> list[tuple]:
    if source == "failures":
        return con.execute("""
            SELECT sector, COUNT(*) as n FROM failures_unified
            WHERE sector IS NOT NULL AND sector != 'Other'
            GROUP BY sector ORDER BY n DESC LIMIT 8
        """).fetchall()
    return con.execute("""
        SELECT sector, COUNT(*) as n FROM unified_opportunities
        WHERE source = ? AND sector IS NOT NULL AND sector != 'Other'
        GROUP BY sector ORDER BY n DESC LIMIT 8
    """, [source]).fetchall()

def _raw_files(source: str) -> list[Path]:
    raw_dir = ROOT / "backend" / "data" / "raw" / source
    if not raw_dir.exists():
        return []
    return sorted(raw_dir.glob("*.json"))

def _probe_grants() -> tuple[bool, str, Optional[dict]]:

    try:
        resp = httpx.post(
            "https://api.grants.gov/v1/api/search2",
            json={"keyword": "artificial intelligence", "rows": 1, "startRecordNum": 0},
            timeout=15,
        )
        resp.raise_for_status()
        data  = resp.json()
        hits  = data.get("data", {}).get("oppHits", [])
        total = data.get("data", {}).get("totalRecords", 0)
        if hits:
            sample = hits[0]
            return True, f"{total} total results for 'artificial intelligence'", {
                "oppNum":     sample.get("oppNum"),
                "title":      sample.get("title", "")[:80],
                "agencyName": sample.get("agencyName", ""),
                "openDate":   sample.get("openDate"),
                "closeDate":  sample.get("closeDate"),
                "awardCeiling": sample.get("awardCeiling"),
            }
        return False, "API returned 0 hits", None
    except Exception as e:
        return False, str(e), None

def _probe_grants_detail(opp_num: str) -> tuple[bool, str, Optional[dict]]:

    try:
        resp = httpx.get(
            "https://api.grants.gov/v1/api/fetchOpportunity",
            params={"oppNum": opp_num},
            timeout=15,
        )
        resp.raise_for_status()
        data   = resp.json()
        detail = data.get("data", {}).get("oppDetails") or data.get("data") or {}
        if detail:
            return True, f"Detail fetched for {opp_num}", {
                "synopsis_len":  len(detail.get("synopsis") or ""),
                "has_cfda":      bool(detail.get("cfdaNumbers")),
                "has_eligibility": bool(detail.get("eligibilities")),
                "estimated_funding": detail.get("estimatedFunding"),
            }
        return False, f"Detail API returned empty for {opp_num}", None
    except Exception as e:
        return False, str(e), None

def _probe_sam() -> tuple[bool, str, Optional[dict]]:

    api_key = os.getenv("SAM_API_KEY", "").strip()
    if not api_key:
        return False, "SAM_API_KEY not set in .env", None
    try:
        today  = date.today()
        params = {
            "api_key":    api_key,
            "limit":      1,
            "naicsCode":  "541511",
            "postedFrom": (today - timedelta(days=90)).strftime("%m/%d/%Y"),
            "postedTo":   today.strftime("%m/%d/%Y"),
            "ptype":      "o,p,k,r,s",
        }
        resp = httpx.get("https://api.sam.gov/prod/opportunities/v2/search",
                         params=params, timeout=15)
        resp.raise_for_status()
        data  = resp.json()
        opps  = data.get("opportunitiesData", [])
        total = data.get("totalRecords", 0)
        if opps:
            s = opps[0]
            return True, f"{total} total for NAICS 541511 (last 90 days)", {
                "noticeId":    s.get("noticeId", "")[:20],
                "title":       s.get("title", "")[:80],
                "department":  s.get("department", ""),
                "postedDate":  s.get("postedDate"),
                "responseDeadLine": s.get("responseDeadLine"),
                "type":        s.get("type"),
            }
        return False, f"SAM returned 0 results for NAICS 541511 (last 90 days)", None
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        if code == 401:
            return False, "SAM_API_KEY rejected (401) — check your key", None
        if code == 429:
            return False, "SAM rate limit hit (429) — wait 24h or check usage", None
        return False, f"SAM HTTP {code}: {e}", None
    except Exception as e:
        return False, str(e), None

def _probe_openalex() -> tuple[bool, str, Optional[dict]]:

    email = os.getenv("OPENALEX_EMAIL", "findout@example.com")
    try:
        resp = httpx.get(
            "https://api.openalex.org/works",
            params={
                "search": "large language model",
                "per-page": 1,
                "mailto": email,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data  = resp.json()
        items = data.get("results", [])
        meta  = data.get("meta", {})
        total = meta.get("count", 0)
        if items:
            w = items[0]
            has_abstract = bool(
                w.get("abstract_inverted_index") or w.get("abstract")
            )
            return True, f"{total:,} total results for 'large language model'", {
                "id":           w.get("id", "").split("/")[-1],
                "title":        (w.get("display_name") or "")[:80],
                "year":         w.get("publication_year"),
                "cited_by":     w.get("cited_by_count", 0),
                "has_abstract": has_abstract,
                "has_topics":   bool(w.get("topics")),
                "n_authors":    len(w.get("authorships") or []),
            }
        return False, "OpenAlex returned 0 results", None
    except Exception as e:
        return False, str(e), None

def _probe_pubmed() -> tuple[bool, str, Optional[dict]]:

    ncbi_key = os.getenv("NCBI_API_KEY", "").strip()
    params = {
        "db": "pubmed", "term": "mRNA vaccine[Title]",
        "retmax": 1, "retmode": "json",
    }
    if ncbi_key:
        params["api_key"] = ncbi_key
    try:
        resp = httpx.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params=params, timeout=15,
        )
        resp.raise_for_status()
        data  = resp.json()
        count = data.get("esearchresult", {}).get("count", "0")
        ids   = data.get("esearchresult", {}).get("idlist", [])
        return True, f"{count} PubMed results for 'mRNA vaccine'", {
            "sample_pmid": ids[0] if ids else None,
            "api_key_used": bool(ncbi_key),
        }
    except Exception as e:
        return False, str(e), None

def _probe_patentsview() -> tuple[bool, str, Optional[dict]]:

    api_key = os.getenv("PATENTSVIEW_API_KEY", "").strip()
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-Api-Key"] = api_key
    try:
        resp = httpx.post(
            "https://search.patentsview.org/api/v1/patent/",
            json={
                "q": {"_begins": {"cpc_subgroup_id": "G06N"}},
                "f": ["patent_id", "patent_title", "patent_abstract", "patent_date"],
                "o": {"per_page": 1, "sort": [{"patent_date": "desc"}]},
            },
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        data    = resp.json()
        patents = data.get("patents") or []
        total   = data.get("total_patent_count", 0)
        if patents:
            p = patents[0]
            return True, f"{total:,} total G06N (AI/ML) patents", {
                "patent_id":    p.get("patent_id"),
                "title":        (p.get("patent_title") or "")[:80],
                "date":         p.get("patent_date"),
                "has_abstract": bool(p.get("patent_abstract")),
                "api_key_used": bool(api_key),
            }
        return False, f"PatentsView returned 0 patents for G06N", None
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 503:
            return False, "PatentsView returned 503 — API may still be migrating", None
        return False, f"PatentsView HTTP {e.response.status_code}: {e}", None
    except Exception as e:
        return False, str(e), None

def _probe_failory() -> tuple[bool, str, Optional[dict]]:

    try:
        resp = httpx.get(
            "https://www.failory.com/cemetery",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
            follow_redirects=True,
        )
        resp.raise_for_status()
        from bs4 import BeautifulSoup
        soup  = BeautifulSoup(resp.text, "html.parser")
        cards = soup.find_all("a", class_=lambda c: c and "cemetery-card-link-block" in c)

        if "cloudflare" in resp.text.lower() and len(cards) == 0:
            return False, "Failory blocked by Cloudflare — scrape will fail", None

        if cards:
            first = cards[0]
            def gf(name):
                el = first.find(attrs={"fs-list-field": name})
                return el.get_text(strip=True) if el else ""
            return True, f"{len(cards)} cards found on page 1", {
                "sample_name":     gf("title"),
                "sample_category": gf("category"),
                "sample_country":  gf("country"),
                "sample_funding":  gf("funding"),
                "sample_closed":   gf("closed"),
            }
        return False, "No cemetery cards found — page structure may have changed", None
    except Exception as e:
        return False, str(e), None

def _probe_cbinsights() -> tuple[bool, str, Optional[dict]]:

    cb_dir = ROOT / "backend" / "data" / "raw" / "failures" / "cbinsights"
    if not cb_dir.exists():
        return False, f"cbinsights/ folder not found at {cb_dir}", None
    csvs = list(cb_dir.glob("*.csv"))
    if not csvs:
        return False, f"No CSV files found in {cb_dir}", None

    import csv
    first = sorted(csvs)[0]
    try:
        with open(first, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows   = list(reader)
        return True, f"{len(csvs)} CSV files, {len(rows)} rows in {first.name}", {
            "files":       [f.name for f in sorted(csvs)],
            "sample_name": rows[0].get("Name", "?") if rows else "?",
            "columns":     list(rows[0].keys())[:6] if rows else [],
        }
    except Exception as e:
        return False, f"Failed to read {first.name}: {e}", None

def check_source(source: str, con, test_api: bool, n_samples: int):
    banner = f"\n{BOLD}{CYAN}{'━'*60}{RESET}"
    print(banner)
    print(f"{BOLD}  SOURCE: {source.upper()}{RESET}")
    print(f"{CYAN}{'━'*60}{RESET}")

    print(f"\n  {BOLD}[1] Connectivity{RESET}")
    if test_api:
        if source == "grants":
            reachable, msg, sample = _probe_grants()
            if reachable:
                ok(f"Grants.gov search API: {msg}")
                if sample and sample.get("oppNum"):

                    det_ok, det_msg, det_sample = _probe_grants_detail(sample["oppNum"])
                    if det_ok:
                        ok(f"Grants.gov detail API: {det_msg}")
                        if det_sample:
                            info(f"  synopsis_len={det_sample['synopsis_len']} chars | "
                                 f"has_cfda={det_sample['has_cfda']} | "
                                 f"has_eligibility={det_sample['has_eligibility']}")
                    else:
                        warn(f"Detail API: {det_msg}")
                if sample:
                    info(f"  Sample: [{sample.get('oppNum')}] {sample.get('title')}")
            else:
                fail(f"Grants.gov unreachable: {msg}")

        elif source == "sam":
            reachable, msg, sample = _probe_sam()
            if reachable:
                ok(f"SAM.gov API: {msg}")
                if sample:
                    info(f"  Sample: [{sample.get('type')}] {sample.get('title')}")
            else:
                fail(f"SAM.gov: {msg}")

        elif source == "research":
            reachable, msg, sample = _probe_openalex()
            if reachable:
                ok(f"OpenAlex API: {msg}")
                if sample:
                    info(f"  Sample: [{sample.get('id')}] {sample.get('title')}")
                    info(f"  abstract={sample['has_abstract']} | "
                         f"topics={sample['has_topics']} | "
                         f"authors={sample['n_authors']}")
            else:
                fail(f"OpenAlex: {msg}")

            pub_ok, pub_msg, pub_sample = _probe_pubmed()
            if pub_ok:
                ok(f"PubMed fallback: {pub_msg}")
            else:
                warn(f"PubMed: {pub_msg} (fallback only — not critical)")

        elif source == "patents":
            reachable, msg, sample = _probe_patentsview()
            if reachable:
                ok(f"PatentsView API: {msg}")
                if sample:
                    info(f"  Sample: [{sample.get('patent_id')}] {sample.get('title')}")
                    info(f"  has_abstract={sample['has_abstract']} | "
                         f"api_key_used={sample['api_key_used']}")
            else:
                warn(f"PatentsView: {msg}")
                warn("  Patents are optional — this will not block other sources")

        elif source == "failures":
            cb_ok, cb_msg, cb_sample = _probe_cbinsights()
            if cb_ok:
                ok(f"CB Insights CSVs: {cb_msg}")
                if cb_sample:
                    info(f"  Files: {cb_sample['files']}")
                    info(f"  Sample company: {cb_sample['sample_name']}")
                    info(f"  Columns (first 6): {cb_sample['columns']}")
            else:
                fail(f"CB Insights: {cb_msg}")

            fail_ok, fail_msg, fail_sample = _probe_failory()
            if fail_ok:
                ok(f"Failory cemetery: {fail_msg}")
                if fail_sample:
                    info(f"  Sample: {fail_sample['sample_name']} "
                         f"({fail_sample['sample_category']}, "
                         f"{fail_sample['sample_country']}, "
                         f"closed {fail_sample['sample_closed']})")
            else:
                warn(f"Failory: {fail_msg}")
                warn("  If Cloudflare blocking, try --no-failory flag in ingest.py")
    else:
        info("API check skipped (--no-api)")

    print(f"\n  {BOLD}[2] Raw files on disk{RESET}")
    src_name = "failures" if source == "failures" else source
    files = _raw_files(src_name)
    if files:
        latest = files[-1]
        size   = latest.stat().st_size / 1024
        try:
            data    = json.loads(latest.read_text())
            n_recs  = len(data) if isinstance(data, list) else "?"
        except Exception:
            n_recs = "?"
        ok(f"{len(files)} file(s) on disk | latest: {latest.name} "
           f"({size:.0f} KB, ~{n_recs} records)")
    else:
        warn(f"No raw files on disk for '{src_name}' — run ingest.py first")

    print(f"\n  {BOLD}[3] Database counts{RESET}")
    counts = _db_counts(con, source)
    total  = counts["total"]
    enr    = counts["enriched"]

    if total == 0:
        fail(f"0 rows in DB — run: python scripts/ingest.py --source {source}")
    elif total < 10:
        warn(f"Only {total} rows — very low, data may be incomplete")
    else:
        ok(f"{total:,} rows in {'failures_unified' if source == 'failures' else 'unified_opportunities'}")

    if source == "failures" and counts.get("by_sub_source"):
        for sub, n in counts["by_sub_source"]:
            info(f"  sub-source '{sub}': {n} rows")

    if enr == 0:
        fail(f"0 enriched_details rows — full_text and summary are missing")
    elif enr < total * 0.5:
        warn(f"enriched_details: {enr}/{total} rows ({enr/total*100:.0f}%) — many records missing full text")
    else:
        ok(f"enriched_details: {enr}/{total} rows ({enr/max(total,1)*100:.0f}%) have full text")

    print(f"\n  {BOLD}[4] Field completeness{RESET}")
    completeness = _field_completeness(con, source)
    if completeness:
        for field, pct in completeness.items():
            if pct >= 80:
                ok(f"{field:<25} {pct}%")
            elif pct >= 40:
                warn(f"{field:<25} {pct}% — partially populated")
            else:
                fail(f"{field:<25} {pct}% — most records missing this field")
    else:
        warn("Completeness check skipped — no rows in DB")

    print(f"\n  {BOLD}[5] Full-text quality (enriched_details){RESET}")
    quality = _enrichment_quality(con, source)
    if quality:
        has_ft_pct = round(quality["has_full_text"] / quality["total"] * 100, 1)
        has_sum_pct= round(quality["has_summary"]   / quality["total"] * 100, 1)
        ok(f"Records with full_text:  {quality['has_full_text']}/{quality['total']} ({has_ft_pct}%)")
        ok(f"Records with summary:    {quality['has_summary']}/{quality['total']} ({has_sum_pct}%)")
        avg = quality["avg_ft_len"]
        mn  = quality["min_ft_len"]
        mx  = quality["max_ft_len"]
        if avg < 100:
            fail(f"avg full_text length: {avg} chars — too short, detail fetch may not be working")
        elif avg < 300:
            warn(f"avg full_text length: {avg} chars — low, check detail API responses")
        else:
            ok(f"avg full_text length: {avg:,} chars (min={mn}, max={mx:,})")
    else:
        warn("No enriched_details rows to assess quality")

    print(f"\n  {BOLD}[6] Sector distribution (top 8){RESET}")
    sectors = _sector_distribution(con, source)
    if sectors:
        for sector_name, n in sectors:
            info(f"  {sector_name:<35} {n:>5} records")

        other_count = con.execute(
            "SELECT COUNT(*) FROM unified_opportunities WHERE source = ? AND sector = 'Other'",
            [source]
        ).fetchone()[0] if source != "failures" else con.execute(
            "SELECT COUNT(*) FROM failures_unified WHERE sector = 'Other'"
        ).fetchone()[0]
        if total > 0 and other_count / total > 0.3:
            warn(f"{other_count} records tagged 'Other' ({other_count/total*100:.0f}%) "
                 "— SECTOR_MAP may need more keywords")
        elif total > 0:
            ok(f"Sector tagging looks healthy "
               f"({other_count} 'Other' = {other_count/total*100:.0f}%)")
    else:
        warn("No sector data available")

    print(f"\n  {BOLD}[7] Sample records from DB{RESET}")
    samples = _sample_records(con, source, n=n_samples)
    if samples:
        for i, rec in enumerate(samples, 1):
            print(f"\n  {CYAN}── Record {i} ──{RESET}")
            if source == "failures":
                print(f"    company:  {rec.get('company_name','')}")
                print(f"    sector:   {rec.get('sector','')}")
                print(f"    year:     {rec.get('year_failed','')}")
                print(f"    funding:  ${rec.get('funding_raised_usd') or 'unknown'}")
                print(f"    reasons:  {rec.get('failure_reasons','')}")
                lesson = str(rec.get("key_lesson",""))
                print(f"    lesson:   {lesson[:120]}{'...' if len(lesson)>120 else ''}")
            else:
                print(f"    title:    {str(rec.get('title',''))[:80]}")
                print(f"    sector:   {rec.get('sector','')}")
                print(f"    agency:   {str(rec.get('agency',''))[:50]}")
                print(f"    posted:   {rec.get('posted_date','')}")
                print(f"    closes:   {rec.get('close_date','')}")
                amt = rec.get("funding_max")
                print(f"    funding:  {'${:,.0f}'.format(amt) if amt else 'N/A'}")
                desc = str(rec.get("description",""))
                print(f"    desc:     {desc[:120]}{'...' if len(desc)>120 else ''}")
    else:
        warn("No sample records available (DB may be empty)")

ALL_SOURCES = ["grants", "sam", "research", "patents", "failures"]

def main():
    parser = argparse.ArgumentParser(
        description="Deep diagnostic for every Findout data source",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source",   choices=ALL_SOURCES, default=None)
    parser.add_argument("--no-api",   action="store_true")
    parser.add_argument("--sample",   type=int, default=2)
    args = parser.parse_args()

    sources = [args.source] if args.source else ALL_SOURCES

    print(f"\n{BOLD}{'═'*60}")
    print(f"  FINDOUT — DATA SOURCE VERIFICATION")
    print(f"  Sources: {', '.join(sources)}")
    print(f"  API checks: {'disabled' if args.no_api else 'enabled'}")
    print(f"{'═'*60}{RESET}")

    con = get_db()
    create_schema(con)

    for source in sources:
        check_source(
            source    = source,
            con       = con,
            test_api  = not args.no_api,
            n_samples = args.sample,
        )

    print(f"\n{BOLD}{'═'*60}")
    print(f"  VERIFICATION SUMMARY")
    print(f"{'═'*60}{RESET}")

    if _failures:
        print(f"\n{RED}{BOLD}  {len(_failures)} check(s) FAILED:{RESET}")
        for f in _failures:
            print(f"  {RED}✗{RESET} {f}")
        print(f"\n  Run {CYAN}python scripts/ingest.py{RESET} to populate missing data.\n")
        sys.exit(1)
    else:
        print(f"\n{GREEN}{BOLD}  All checks passed.{RESET}\n")
        sys.exit(0)

if __name__ == "__main__":
    main()