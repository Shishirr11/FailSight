import json
import re
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from storage.db import get_db

PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

SECTOR_MAP: dict[str, str] = {

    "large language model":        "AI & Machine Learning",
    "natural language processing": "AI & Machine Learning",
    "computer vision":             "AI & Machine Learning",
    "deep learning":               "AI & Machine Learning",
    "reinforcement learning":      "AI & Machine Learning",
    "federated learning":          "AI & Machine Learning",
    "generative ai":               "AI & Machine Learning",
    "foundation model":            "AI & Machine Learning",
    "artificial intelligence":     "AI & Machine Learning",
    "machine learning":            "AI & Machine Learning",

    "zero trust":                  "Cybersecurity",
    "network security":            "Cybersecurity",
    "intrusion detection":         "Cybersecurity",
    "cybersecurity":               "Cybersecurity",
    "cyber security":              "Cybersecurity",
    "adversarial ml":              "Cybersecurity",
    "threat intelligence":         "Cybersecurity",

    "energy storage":              "Clean Energy",
    "hydrogen fuel":               "Clean Energy",
    "solar photovoltaic":          "Clean Energy",
    "solar":                       "Clean Energy",
    "wind energy":                 "Clean Energy",
    "grid modernization":          "Clean Energy",
    "smart grid":                  "Clean Energy",
    "clean energy":                "Clean Energy",
    "renewable energy":            "Clean Energy",
    "renewable":                   "Clean Energy",
    "fuel cell":                   "Clean Energy",
    "lithium battery":             "Clean Energy",

    "carbon capture":              "Climate Technology",
    "carbon sequestration":        "Climate Technology",
    "climate change":              "Climate Technology",
    "climate technology":          "Climate Technology",
    "climate adaptation":          "Climate Technology",
    "net zero":                    "Climate Technology",

    "crispr":                      "Biotechnology",
    "gene therapy":                "Biotechnology",
    "gene editing":                "Biotechnology",
    "synthetic biology":           "Biotechnology",
    "metabolic engineering":       "Biotechnology",
    "protein structure":           "Biotechnology",
    "drug discovery":              "Biotechnology",
    "mrna":                        "Biotechnology",
    "vaccine":                     "Biotechnology",
    "biotechnology":               "Biotechnology",
    "biotech":                     "Biotechnology",
    "genomics":                    "Biotechnology",

    "digital health":              "Health Technology",
    "telehealth":                  "Health Technology",
    "telemedicine":                "Health Technology",
    "remote patient monitoring":   "Health Technology",
    "precision medicine":          "Health Technology",
    "medical imaging":             "Health Technology",
    "wearable biosensor":          "Health Technology",
    "mental health technology":    "Health Technology",
    "medical device":              "Health Technology",
    "health technology":           "Health Technology",
    "health tech":                 "Health Technology",

    "quantum computing":           "Quantum Computing",
    "quantum cryptography":        "Quantum Computing",
    "quantum error correction":    "Quantum Computing",
    "quantum communication":       "Quantum Computing",
    "qubit":                       "Quantum Computing",

    "additive manufacturing":      "Advanced Manufacturing",
    "3d printing":                 "Advanced Manufacturing",
    "robotics":                    "Advanced Manufacturing",
    "autonomous vehicle":          "Advanced Manufacturing",
    "self-driving":                "Advanced Manufacturing",
    "industrial iot":              "Advanced Manufacturing",
    "advanced materials":          "Advanced Manufacturing",
    "composite materials":         "Advanced Manufacturing",
    "domestic manufacturing":      "Advanced Manufacturing",
    "biomanufacturing":            "Advanced Manufacturing",
    "supply chain":                "Advanced Manufacturing",
    "advanced manufacturing":      "Advanced Manufacturing",
    "manufacturing":               "Advanced Manufacturing",

    "semiconductor":               "Advanced Computing",
    "microelectronics":            "Advanced Computing",
    "printed electronics":         "Advanced Computing",
    "edge computing":              "Advanced Computing",
    "photonics":                   "Advanced Computing",
    "neuromorphic":                "Advanced Computing",
    "high performance computing":  "Advanced Computing",
    "chip design":                 "Advanced Computing",
    "integrated circuit":          "Advanced Computing",
    "blockchain":                  "Advanced Computing",

    "small satellite":             "Aerospace & Defense",
    "cubesat":                     "Aerospace & Defense",
    "satellite":                   "Aerospace & Defense",
    "hypersonic":                  "Aerospace & Defense",
    "directed energy":             "Aerospace & Defense",
    "autonomous drone":            "Aerospace & Defense",
    "uav":                         "Aerospace & Defense",
    "aerospace":                   "Aerospace & Defense",
    "defense technology":          "Aerospace & Defense",
    "dual use technology":         "Aerospace & Defense",
    "space technology":            "Aerospace & Defense",
    "space":                       "Aerospace & Defense",
    "biodefense":                  "Aerospace & Defense",

    "controlled environment agriculture": "Agriculture Technology",
    "specialty crop":              "Agriculture Technology",
    "precision agriculture":       "Agriculture Technology",
    "agricultural biotechnology":  "Agriculture Technology",
    "agricultural technology":     "Agriculture Technology",
    "food safety technology":      "Agriculture Technology",
    "food systems technology":     "Agriculture Technology",
    "food technology":             "Agriculture Technology",
    "food security":               "Agriculture Technology",
    "rural development":           "Agriculture Technology",
    "agtech":                      "Agriculture Technology",

    "electric vehicle infrastructure": "Transportation",
    "autonomous transportation":   "Transportation",
    "transportation safety":       "Transportation",
    "bridge infrastructure":       "Transportation",
    "port modernization":          "Transportation",
    "broadband infrastructure":    "Infrastructure",

    "building energy efficiency":  "Clean Energy",
    "smart building":              "Clean Energy",
    "advanced hvac":               "Clean Energy",
    "energy efficient":            "Clean Energy",
    "microgrid":                   "Clean Energy",

    "flood resilience":            "Climate Technology",
    "wildfire technology":         "Climate Technology",
    "drought resilience":          "Climate Technology",
    "environmental remediation":   "Climate Technology",
    "environmental justice":       "Climate Technology",

    "affordable housing":          "Community Development",
    "community resilience":        "Community Development",
    "disaster preparedness":       "Community Development",

    "workforce development":       "Education",
    "apprenticeship":              "Education",
    "skills training":             "Education",
    "stem education":              "Education",

    "financial technology":        "Fintech",
    "fintech":                     "Fintech",
    "decentralized finance":       "Fintech",
    "defi":                        "Fintech",
    "insurtech":                   "Fintech",

    "small business":              "Small Business",
    "technology commercialization":"Small Business",
}

def infer_sector(text: str, keyword: str = "") -> str:
    """Best-effort sector label from title / description / keyword / naics."""
    combined = (text + " " + keyword).lower()
    for kw, sector in SECTOR_MAP.items():
        if kw in combined:
            return sector
    return "Other"

def _safe_date(val) -> Optional[str]:
    if not val:
        return None
    try:
        return pd.to_datetime(val).date().isoformat()
    except Exception:
        return None

def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(str(val).replace("$", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None

def _extractive_summary(text: str, n_sentences: int = 4) -> str:
    """
    Pure-Python word-frequency extractive summary. No external calls.
    Scores each sentence by its content-word frequency, returns top-n
    sentences in original reading order.
    """
    if not text or len(text) < 120:
        return text or ""

    STOPWORDS = {
        "the","a","an","and","or","but","in","on","at","to","for","of","with",
        "is","are","was","were","be","been","being","have","has","had","do",
        "does","did","will","would","could","should","may","might","shall",
        "this","that","these","those","it","its","we","our","you","your",
        "they","their","he","she","his","her","i","my","me","us","by","from",
        "as","not","no","so","if","then","than","also","just","more","about",
        "which","who","what","how","when","where","all","any","each","both",
    }

    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    sentences = [s.strip() for s in sentences if len(s.split()) > 5]
    if len(sentences) <= n_sentences:
        return " ".join(sentences)

    words = re.findall(r'\b[a-z]{3,}\b', text.lower())
    freq  = {}
    for w in words:
        if w not in STOPWORDS:
            freq[w] = freq.get(w, 0) + 1

    scored = []
    for sent in sentences:
        ws    = re.findall(r'\b[a-z]{3,}\b', sent.lower())
        score = sum(freq.get(w, 0) for w in ws if w not in STOPWORDS)
        score = score / max(len(ws), 1)
        scored.append(score)

    top_idxs = sorted(
        sorted(range(len(scored)), key=lambda i: scored[i], reverse=True)[:n_sentences]
    )
    return " ".join(sentences[i] for i in top_idxs)

def _norm_grants(r: dict) -> tuple[dict, dict]:
    title   = r.get("title") or r.get("oppTitle") or ""
    keyword = r.get("_keyword", "")
    sector  = r.get("_sector") or infer_sector(title, keyword)

    full_text  = r.get("_full_text", "") or (
        " ".join(filter(None, [
            r.get("synopsis"), r.get("description"), r.get("additionalInfo"),
            r.get("programDescription"), r.get("objectives"),
            r.get("eligibilities"), r.get("costSharing"),
        ]))
    )

    all_kws = r.get("_keywords") or ([keyword] if keyword else [])
    opp = {
        "opp_id":      r["_foip_id"],
        "source":      "grants",
        "title":       title,
        "description": r.get("synopsis") or r.get("description") or "",
        "sector":      sector,
        "naics_code":  None,
        "posted_date": _safe_date(r.get("openDate")),
        "close_date":  _safe_date(r.get("closeDate")),
        "funding_min": _safe_float(r.get("awardFloor") if r.get("awardFloor") != "none" else None),
        "funding_max": _safe_float(r.get("awardCeiling") if r.get("awardCeiling") != "none" else None),

        "agency":      r.get("agency") or r.get("agencyName") or "",
        "geography":   "US",
        "eligibility": r.get("eligibilities") or "",
        "tags":        list(dict.fromkeys(filter(None, all_kws + [sector]))),
        "raw_json":    json.dumps(r),
    }
    key_fields = {
        "opp_num":            r.get("oppNum"),
        "agency_code":        r.get("agencyCode"),
        "award_floor":        r.get("awardFloor"),
        "award_ceiling":      r.get("awardCeiling"),
        "estimated_funding":  r.get("estimatedFunding"),
        "expected_awards":    r.get("expectedNumberOfAwards"),
        "open_date":          r.get("openDate"),
        "close_date":         r.get("closeDate"),
        "eligibilities":      r.get("eligibilities"),
        "cost_sharing":       r.get("costSharing"),
        "funding_instrument": r.get("fundingInstrumentType"),
        "activity_category":  r.get("activityCategory"),
        "cfda_numbers":       r.get("cfdaNumbers"),
        "program_url":        r.get("programUrl"),
        "grantor_contact":    r.get("grantorContactName"),
        "grantor_email":      r.get("grantorContactEmail"),
    }
    enr = {
        "record_id":   r["_foip_id"],
        "record_type": "opportunity",
        "source":      "grants",
        "full_text":   full_text[:50_000],
        "summary":     _extractive_summary(full_text),
        "key_fields":  key_fields,
    }
    return opp, enr

def _norm_sam(r: dict) -> tuple[dict, dict]:
    title  = r.get("title") or ""
    naics  = r.get("naicsCode") or r.get("_naics_queried") or ""

    agency = r.get("_agency") or ""

    amt    = r.get("_funding") or _safe_float((r.get("award") or {}).get("amount"))

    desc_text = r.get("_description_text", "")
    sector = r.get("_sector") or infer_sector(
        title + " " + naics + " " + desc_text[:200], naics
    )

    full_text = r.get("_full_text", "")

    opp = {
        "opp_id":      "sam_" + r.get("noticeId", ""),
        "source":      "sam",
        "title":       title,

        "description": desc_text[:1000] if desc_text else "",
        "sector":      sector,
        "naics_code":  naics,
        "posted_date": _safe_date(r.get("postedDate")),
        "close_date":  _safe_date(r.get("responseDeadLine") or r.get("archiveDate")),
        "funding_min": None,
        "funding_max": amt,
        "agency":      agency,
        "geography":   (
            (r.get("placeOfPerformance") or {})
            .get("state", {}).get("name", "")
            or "US"
        ),
        "eligibility": r.get("typeOfSetAsideDescription") or "",
        "tags":        list(filter(None, [naics, sector])),
        "raw_json":    json.dumps(r, default=str),
    }
    key_fields = {
        "notice_id":           r.get("noticeId"),
        "solicitation_number": r.get("solicitationNumber") or r.get("sol"),
        "notice_type":         r.get("type"),
        "set_aside":           r.get("typeOfSetAside"),
        "set_aside_desc":      r.get("typeOfSetAsideDescription"),
        "naics_code":          naics,
        "classification_code": r.get("classificationCode"),
        "full_parent_path":    r.get("fullParentPathName"),
        "pop_city":  (r.get("placeOfPerformance") or {}).get("city", {}).get("name"),
        "pop_state": (r.get("placeOfPerformance") or {}).get("state", {}).get("name"),
        "contract_value":      amt,
        "award_date":          (r.get("award") or {}).get("date"),
        "archive_date":        r.get("archiveDate"),
        "contact":             r.get("_contact"),
        "ui_link":             r.get("uiLink"),
        "resource_links":      r.get("resourceLinks"),
    }
    enr = {
        "record_id":   opp["opp_id"],
        "record_type": "opportunity",
        "source":      "sam",
        "full_text":   full_text[:50_000],
        "summary":     _extractive_summary(full_text),
        "key_fields":  key_fields,
    }
    return opp, enr

def _norm_sbir(r: dict) -> tuple[dict, dict]:
    title   = r.get("Award Title") or r.get("award_title") or ""
    sector  = r.get("_sector") or infer_sector(title, "")
    firm    = r.get("Company") or r.get("firm") or ""
    agency  = r.get("Agency") or r.get("agency") or ""
    branch  = r.get("Branch") or r.get("branch") or ""
    state   = r.get("State") or r.get("state") or "US"
 
    def _to_str(v):
        if isinstance(v, list): return " ".join(str(x) for x in v if x)
        return str(v) if v else ""

    abstract = _to_str(r.get("abstractText") or r.get("publicAbstractText"))
    awardee  = _to_str(r.get("awardeeName") or r.get("orgLongName"))
    pi       = _to_str(r.get("pdPIName") or f"{r.get('piFirstName','')} {r.get('piLastName','')}".strip())
    agency   = _to_str(r.get("agency"))

    amount = _safe_float(
        r.get("fundsObligatedAmt") or
        r.get("fundsObligated") or
        r.get("estimatedTotalAmt")
    )

    full_text = " ".join(filter(None, [
        _to_str(r.get("title")),
        abstract, awardee, pi,
        r.get("_keyword", ""),
        _to_str(r.get("primaryProgram")),
        _to_str(r.get("fundProgramName")),
    ]))
 
    year = r.get("Award Year") or r.get("award_year") or ""
    posted_date = _safe_date(str(year)) if year else None
 
    close_date = _safe_date(
        r.get("Contract End Date") or r.get("contract_end_date")
    )
 
    full_text = " ".join(filter(None, [
        title, abstract, firm, agency, branch,
        r.get("_keyword", ""),
    ]))
 
    agency_str = " — ".join(filter(None, [agency, branch, firm]))
 
    opp = {
        "opp_id":      r["_foip_id"],
        "source":      "sbir",
        "title":       title,
        "description": abstract[:1000] if abstract else "",
        "sector":      sector,
        "naics_code":  None,
        "posted_date": posted_date,
        "close_date":  close_date,
        "funding_min": None,
        "funding_max": amount,
        "agency":      f"NSF — {awardee}"[:300] if awardee else "NSF",
        "geography":   _to_str(r.get("perfStateCode") or r.get("awardeeStateCode")) or "US",
        "eligibility": "Small Business (SBIR/STTR)",
        "tags":        list(filter(None, [sector, "SBIR", "small business", agency])),
        "raw_json":    json.dumps(r, default=str),
    }
 
    key_fields = {
        "firm":                firm,
        "agency":              agency,
        "branch":              branch,
        "phase":               r.get("Phase") or r.get("phase"),
        "program":             r.get("Program") or r.get("program") or "SBIR",
        "award_amount":        amount,
        "award_year":          year,
        "contract":            r.get("Contract") or r.get("contract"),
        "solicitation_number": r.get("Solicitation Number") or r.get("solicitation_number"),
        "topic_code":          r.get("Topic Code") or r.get("topic_code"),
        "pi_name":             r.get("PI Name") or r.get("pi_name"),
        "pi_email":            r.get("PI Email") or r.get("pi_email"),
        "contact_name":        r.get("Contact Name") or r.get("poc_name"),
        "contact_email":       r.get("Contact Email") or r.get("poc_email"),
        "company_url":         r.get("Company Website") or r.get("company_url"),
        "state":               state,
        "hubzone":             r.get("HUBZone Owned") or r.get("hubzone_owned"),
        "women_owned":         r.get("Women Owned") or r.get("women_owned"),
        "num_employees":       r.get("Number Employees") or r.get("number_employees"),
    }
 
    enr = {
        "record_id":   opp["opp_id"],
        "record_type": "opportunity",
        "source":      "sbir",
        "full_text":   full_text[:50_000],
        "summary":     _extractive_summary(full_text),
        "key_fields":  key_fields,
    }
    return opp, enr

def _norm_nsf(r: dict) -> tuple[dict, dict]:
    def s(v):
        if isinstance(v, list): return " ".join(str(x) for x in v if x)
        return str(v).strip() if v else ""

    title    = s(r.get("title"))
    sector   = r.get("_sector") or infer_sector(title, r.get("_keyword", ""))
    awardee  = s(r.get("awardeeName") or r.get("awardee") or r.get("orgLongName"))
    pi       = s(r.get("pdPIName") or f"{r.get('piFirstName','')} {r.get('piLastName','')}".strip())
    abstract = s(r.get("abstractText"))
    program  = s(r.get("program") or r.get("fundProgramName") or r.get("primaryProgram"))
    state    = s(r.get("perfStateCode") or r.get("awardeeStateCode"))

    amount = _safe_float(r.get("fundsObligatedAmt") or r.get("estimatedTotalAmt"))

    full_text = " ".join(filter(None, [
        title, abstract, awardee, pi, program, r.get("_keyword", "")
    ]))

    opp = {
        "opp_id":      r["_foip_id"],
        "source":      "nsf",
        "title":       title,
        "description": abstract[:1000],
        "sector":      sector,
        "naics_code":  None,
        "posted_date": _safe_date(r.get("startDate")),
        "close_date":  _safe_date(r.get("expDate")),
        "funding_min": None,
        "funding_max": amount,
        "agency":      f"NSF — {awardee}"[:300] if awardee else "NSF",
        "geography":   state or "US",
        "eligibility": None,
        "tags":        list(filter(None, [sector, r.get("_keyword", ""), "NSF"])),
        "raw_json":    json.dumps(r, default=str),
    }
    key_fields = {
        "nsf_id":      r.get("id"),
        "awardee":     awardee,
        "pi_name":     pi,
        "pi_email":    s(r.get("piEmail")),
        "amount":      amount,
        "program":     program,
        "cfda":        s(r.get("cfdaNumber")),
        "start_date":  s(r.get("startDate")),
        "exp_date":    s(r.get("expDate")),
        "trans_type":  s(r.get("transType")),
        "dir":         s(r.get("dirAbbr")),
        "div":         s(r.get("divAbbr")),
    }
    enr = {
        "record_id":   opp["opp_id"],
        "record_type": "opportunity",
        "source":      "nsf",
        "full_text":   full_text[:50_000],
        "summary":     _extractive_summary(full_text),
        "key_fields":  key_fields,
    }
    return opp, enr


def _norm_research(r: dict) -> tuple[dict, dict]:
    title   = r.get("display_name") or r.get("title") or ""
    sector  = r.get("_sector") or infer_sector(title, r.get("_query", ""))
    work_id = str(r.get("id", "")).split("/")[-1]
    opp_id  = "research_" + work_id

    abstract = (
        r.get("_abstract_text") or
        r.get("abstract") or
        r.get("_pubmed_abstract") or ""
    )

    parts = [title, abstract]
    for t in (r.get("topics") or [])[:5]:
        parts.append(t.get("display_name", ""))
    for c in (r.get("concepts") or [])[:8]:
        parts.append(c.get("display_name", ""))
    for k in (r.get("keywords") or [])[:10]:
        parts.append(k.get("keyword", ""))
    full_text = " ".join(p for p in parts if p).strip()

    authors = "; ".join(
        f"{a.get('author', {}).get('display_name', '')}"
        for a in (r.get("authorships") or [])[:5]
    )
    institutions = "; ".join(
        inst.get("display_name", "")
        for a in (r.get("authorships") or [])[:3]
        for inst in (a.get("institutions") or [])[:1]
    )
    doi = r.get("doi") or ""
    oa  = (r.get("open_access") or {}).get("oa_url") or ""

    opp = {
        "opp_id":      opp_id,
        "source":      "research",
        "title":       title,
        "description": abstract[:1000] if abstract else "",
        "sector":      sector,
        "naics_code":  None,
        "posted_date": _safe_date(str(r.get("publication_year", ""))),
        "close_date":  None,
        "funding_min": None,
        "funding_max": None,
        "agency":      institutions[:200] if institutions else "",
        "geography":   None,
        "eligibility": None,
        "tags":        list(filter(None, [sector, r.get("_query", "")])),
        "raw_json":    json.dumps(r, default=str),
    }
    key_fields = {
        "openalex_id":   r.get("id"),
        "doi":           doi,
        "oa_url":        oa,
        "cited_by":      r.get("cited_by_count", 0),
        "pub_year":      r.get("publication_year"),
        "authors":       authors,
        "institutions":  institutions,
        "topics":        [t.get("display_name") for t in (r.get("topics") or [])[:10]],
        "concepts":      [c.get("display_name") for c in (r.get("concepts") or [])[:10]],
        "funding":       [f.get("funder_display_name") for f in (r.get("grants") or [])[:5]],
        "pubmed_abstract": r.get("_pubmed_abstract", "")[:2000],
    }
    enr = {
        "record_id":   opp_id,
        "record_type": "opportunity",
        "source":      "research",
        "full_text":   full_text[:50_000],
        "summary":     _extractive_summary(full_text),
        "key_fields":  key_fields,
    }
    return opp, enr

def _norm_patents(r: dict) -> tuple[dict, dict]:
    pid     = r.get("patent_id", "")
    opp_id  = "patent_" + str(pid)
    title   = r.get("patent_title") or ""
    abstract= r.get("patent_abstract") or ""
    sector  = r.get("_sector") or infer_sector(title + " " + abstract)

    assignees = "; ".join(
        a.get("assignee_organization", "")
        for a in (r.get("assignees") or [])[:3]
        if a.get("assignee_organization")
    )
    cpc_titles = "; ".join(
        c.get("cpc_subgroup_title", "")
        for c in (r.get("cpcs") or [])[:5]
        if c.get("cpc_subgroup_title")
    )
    inventors = "; ".join(
        f"{i.get('inventor_first_name','')} {i.get('inventor_last_name','')}".strip()
        for i in (r.get("inventors") or [])[:5]
    )
    full_text = " ".join(filter(None, [title, abstract, cpc_titles]))

    opp = {
        "opp_id":      opp_id,
        "source":      "patents",
        "title":       title,
        "description": abstract[:1000] if abstract else "",
        "sector":      sector,
        "naics_code":  None,
        "posted_date": _safe_date(r.get("patent_date")),
        "close_date":  None,
        "funding_min": None,
        "funding_max": None,
        "agency":      assignees[:200] if assignees else "",
        "geography":   None,
        "eligibility": None,
        "tags":        list(filter(None, [sector])),
        "raw_json":    json.dumps(r, default=str),
    }
    key_fields = {
        "patent_id":   pid,
        "patent_type": r.get("patent_type"),
        "patent_date": r.get("patent_date"),
        "assignees":   assignees,
        "inventors":   inventors,
        "cpc_codes":   [c.get("cpc_subgroup_id") for c in (r.get("cpcs") or [])[:10]],
        "cpc_titles":  cpc_titles,
    }
    enr = {
        "record_id":   opp_id,
        "record_type": "opportunity",
        "source":      "patents",
        "full_text":   full_text[:50_000],
        "summary":     _extractive_summary(full_text),
        "key_fields":  key_fields,
    }
    return opp, enr

def _norm_failure(r: dict) -> tuple[dict, dict]:
    reasons = r.get("failure_reasons") or ["unknown"]
    if isinstance(reasons, str):
        reasons = [x.strip() for x in reasons.split("|") if x.strip()]

    full_text = " ".join(filter(None, [
        r.get("company_name", ""),
        r.get("description", ""),
        r.get("why_failed", ""),
        r.get("key_lesson", ""),
        r.get("full_article", ""),

        r.get("condensed_value_prop", ""),
        r.get("condensed_cause_of_death", ""),
        r.get("difficulty_reason", ""),
        r.get("scalability_reason", ""),
        r.get("market_potential_reason", ""),

        r.get("_detail_market_analysis", ""),
        r.get("_detail_market_potential_full", ""),
        r.get("_detail_difficulty_full", ""),
        r.get("_detail_scalability_full", ""),
        r.get("_detail_rebuild_concept", ""),
    ]))

    fail = {
        "failure_id":         r["failure_id"],
        "company_name":       r.get("company_name", ""),
        "sector":             r.get("sector", "Other"),
        "naics_code":         None,
        "year_founded":       r.get("year_founded"),
        "year_failed":        r.get("year_failed"),
        "funding_raised_usd": r.get("funding_raised_usd"),
        "failure_reasons":    reasons,
        "stage_at_failure":   r.get("stage_at_failure", ""),
        "key_lesson":         r.get("key_lesson", ""),
        "founder_names":      r.get("founder_names", ""),
        "source_url":         r.get("source_url", ""),
        "raw_json":           json.dumps({
            "description":        r.get("description", ""),
            "why_failed":         r.get("why_failed", ""),
            "full_article":       r.get("full_article", ""),
            "outcome":            r.get("outcome", ""),
            "category":           r.get("category", ""),
            "country":            r.get("country", ""),
            "employees":          r.get("employees", ""),
            "funding_range":      r.get("funding_range", ""),
            "num_founders":       r.get("num_founders", ""),
            "num_investors":      r.get("num_investors", ""),
            "num_funding_rounds": r.get("num_funding_rounds", ""),
            "_source":            r.get("_source", "cbinsights"),
            "_file":              r.get("_file", ""),

            "_lootdrop_id":       r.get("_lootdrop_id", ""),

        "market_analysis":        r.get("_detail_market_analysis", "")[:500],
        "market_potential_full":  r.get("_detail_market_potential_full", "")[:500],
        "difficulty_full":        r.get("_detail_difficulty_full", "")[:500],
        "scalability_full":       r.get("_detail_scalability_full", "")[:500],
        "rebuild_concept":        r.get("_detail_rebuild_concept", "")[:500],
    }, default=str),
    }

    key_fields = {
        "description":    r.get("description", ""),
        "why_failed":     r.get("why_failed", ""),
        "outcome":        r.get("outcome", ""),
        "country":        r.get("country", ""),
        "employees":      r.get("employees", ""),
        "funding_range":  r.get("funding_range", ""),
        "num_founders":   r.get("num_founders", ""),
        "num_investors":  r.get("num_investors", ""),
        "_source":        r.get("_source", ""),

        "difficulty":              r.get("difficulty"),
        "difficulty_reason":       r.get("difficulty_reason", ""),
        "scalability":             r.get("scalability"),
        "scalability_reason":      r.get("scalability_reason", ""),
        "market_potential":        r.get("market_potential"),
        "market_potential_reason": r.get("market_potential_reason", ""),
        "product_type":            r.get("product_type", ""),
        "views":                   r.get("views"),

    }

    scores_raw = r.get("_scores", "")
    if scores_raw:
        try:
            scores = json.loads(scores_raw)
            for k, v in scores.items():
                if k not in key_fields or not key_fields[k]:
                    key_fields[k] = v
        except Exception:
            pass

    enr = {
        "record_id":   r["failure_id"],
        "record_type": "failure",
        "source":      r.get("_source", "cbinsights"),
        "full_text":   full_text[:50_000],
        "summary":     _extractive_summary(full_text),
        "key_fields":  key_fields,
    }
    return fail, enr

_NORMALIZERS = {
    "grants":   _norm_grants,
    "sam":      _norm_sam,
    "sbir":     _norm_sbir,
    "nsf":      _norm_nsf,
    "research": _norm_research,
    "patents":  _norm_patents,
    "failures": _norm_failure,
}

_OPP_COLS = [
    "opp_id","source","title","description","sector","naics_code",
    "posted_date","close_date","funding_min","funding_max","agency",
    "geography","eligibility","tags","raw_json",
]
_FAIL_COLS = [
    "failure_id","company_name","sector","naics_code","year_founded",
    "year_failed","funding_raised_usd","failure_reasons","stage_at_failure",
    "key_lesson","founder_names","source_url","raw_json",
]

def write_records(records: list[dict], source: str, con=None) -> int:
    if con is None:
        con = get_db()

    if not records:
        logger.warning(f"write_records({source}): no records — skipping.")
        return 0

    norm_fn = _NORMALIZERS.get(source)
    if not norm_fn:
        logger.error(f"No normalizer for source '{source}'")
        return 0

    is_failure = (source == "failures")
    main_rows, enr_rows = [], []

    for r in records:
        try:
            main_row, enr_row = norm_fn(r)

            pk = main_row.get("failure_id" if is_failure else "opp_id", "")
            if not pk or len(pk) <= (8 if not is_failure else 3):
                continue
            main_rows.append(main_row)
            enr_rows.append(enr_row)
        except Exception as e:
            # logger.warning(f"  Skipping record ({source}): {e}")
            continue

    if not main_rows:
        logger.warning(f"write_records({source}): 0 valid rows after normalization.")
        return 0

    df = pd.DataFrame(main_rows)
    today  = date.today()
    folder = PROCESSED_DIR / source / f"year={today.year}" / f"month={today.month:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    out = folder / "data.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out, compression="snappy")
    logger.debug(f"Parquet → {out} ({len(df)} rows)")

    if is_failure:
        cols = _FAIL_COLS
        table_name = "failures_unified"
        pk_col = "failure_id"
    else:
        cols = _OPP_COLS
        table_name = "unified_opportunities"
        pk_col = "opp_id"

    df_db = df[[c for c in cols if c in df.columns]]
    con.register("_incoming", df_db)
    before = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    col_list = ", ".join(cols)
    if not is_failure:
        select_parts = []
        for c in cols:
            if c in ("posted_date", "close_date"):
                select_parts.append(f"{c}::DATE")
            else:
                select_parts.append(c)
        select_str = ", ".join(select_parts)
    else:
        select_str = col_list

    con.execute(f"""
        INSERT OR IGNORE INTO {table_name} ({col_list}, created_at)
        SELECT {select_str}, NOW() FROM _incoming
    """)

    after    = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    new_rows = after - before

    _upsert_enriched_batch(con, enr_rows)

    logger.success(
        f"write_records({source}): +{new_rows} new rows "
        f"| {len(enr_rows)} enriched_details upserted"
    )
    return new_rows

def _is_writable_table(con, table_name: str) -> bool:
    """Returns True only if the object is a real base table (not a view)."""
    result = con.execute("""
        SELECT table_type FROM information_schema.tables
        WHERE table_name = ?
    """, [table_name]).fetchone()
    return result is not None and result[0] == 'BASE TABLE'


def _upsert_enriched_batch(con, rows: list[dict]) -> None:
    if not rows:
        return

    if not _is_writable_table(con, 'enriched_details'):
        logger.debug("enriched_details is a view — skipping enriched upsert (R2 mode)")
        return

    from datetime import datetime
    now = datetime.now().isoformat()
    for r in rows:
        con.execute("""
            INSERT INTO enriched_details
                (record_id, record_type, source, full_text, summary,
                 key_fields, enriched_at, enrichment_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'done')
            ON CONFLICT (record_id, source) DO UPDATE SET
                full_text         = excluded.full_text,
                summary           = excluded.summary,
                key_fields        = excluded.key_fields,
                enriched_at       = excluded.enriched_at,
                enrichment_status = 'done',
                error_msg         = NULL
        """, [
            r["record_id"], r["record_type"], r["source"],
            r.get("full_text", ""),
            r.get("summary", ""),
            json.dumps(r.get("key_fields", {}), default=str),
            now,
        ])