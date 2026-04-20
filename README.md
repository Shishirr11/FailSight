# Failsight — Founder's Intelligence Platform

One place to see what funding exists, what companies have failed before you, and whether your idea has a real chance. No paywalls, no signups — all data pulled from public sources and stored locally.

---
## The Trade Off

**Search Index Size**

The TF-IDF matrix is the large thing in the project. At default settings (max_features=50,000, ngram_range=(1,2)), it builds to 6.6GB — too large for any affordable hosting tier. For deployment, rebuild with max_features=5,000, ngram_range=(1,1), and min_df=3. Dropping bigrams is the biggest lever — it roughly halves vocabulary size at the same feature count, giving you 10,000-word effective coverage at a 5,000-slot budget. The resulting matrix is  650MB. Sentence embeddings are disabled in production for the same reason — the all-mpnet-base-v2 model loads ~420MB into RAM on first request. TF-IDF alone handles semantic search adequately at this scale.


---
## Working Stuff

### Data Sources

| Source | Records | What You Get |
|--------|---------|--------------|
| Grants.gov | Federal grants with full descriptions, funding amounts, deadlines |
| SAM.gov | The gov is migrating the webiste to a new domain couldn't get the data yet|
| SBIR.gov | Small business innovation research awards with abstracts |
| NSF Awards | National Science Foundation grants with PI details |
| OpenAlex / PubMed | Research papers with abstracts, authors, citations |
| PatentsView | The gov is migrating the webiste to a new domain couldn't get the data yet |
| CB Insights (CSV) | Failed startups — name, sector, failure reasons |
| Failory | Failed startups — full articles, founders, exact funding |
| LootDrop | Failed startups — failure analysis, market analysis, lessons, scores |

**2,767 failed startups + 30,000+ active opportunities in one local database.**

---

## Pages

**Findout (Home)** — Browse grants, contracts, patents, research, and SBIR awards in source tabs. Search in plain English using TF-IDF + sentence embeddings — not keyword matching. Click any card for the full detail view with source links, contact info, and a Groq-generated "why this matters" blurb tied to your query. The right panel shows a live sector intelligence briefing. Both panels are independently scrollable and resizable by dragging the divider.

**Intelligence** — Google-style search page. Type your startup idea, get a structured verdict. Validate-idea loads first (market exists, confidence level, biggest risk, first grant to apply for). Gap Finder and Grant Match expand below as collapsible sections. All results are powered by Groq reading real data from your local database — no hallucinations.

**Dead Ideas** — Explore all 2,767 failed startups. Filter by sector (chip picker), failure reason, year range, and minimum funding raised. Click any company for the full story — what they tried, what went wrong, what others learned. Detail view is a full-page takeover with a breadcrumb trail back to your previous search.

---

## AI Features

Four intelligence endpoints, all grounded in your local database before calling Groq:

**The Relation** — When you open a detail card, Groq reads the opportunity data and your search query and writes 2–3 sentences explaining why this specific result is relevant to you. Plain prose, no bullet points.

**Idea Validator** — Takes your startup description, searches the grants table and failure post-mortems, and returns: market exists (yes/no), confidence level, a 2-sentence verdict, the biggest risk, and the single best grant to apply for first. Structured fields with risk in red.

**Gap Finder** — For a sector, reads white-space signals, recent research titles, open SAM contracts, and failure reasons. Returns a 2×2 grid: tried and failed / researched but not funded / open demand / non-obvious play. The non-obvious play cell is highlighted — it's the insight a founder wouldn't see just browsing.

**Grant Match** — Paste your project description, Groq compares it against the top 5 matching grants and returns a fit score (1–10), green chips for criteria you meet, red chips for what's missing, and a ranked list of all matches.

**Sector Suggest** — As you type in the sector filter, Groq returns the 3–5 most semantically relevant sectors from the database in real time. Falls back to substring match if Groq is unavailable.

---

## Intelligence Modules

**Risk Scorer** — LOW / MEDIUM / HIGH based on sector failure history and top failure reasons from the failures database.

**Market Validator** — Grades market strength A–D from grant + contract activity, patent density, and research volume. Scores 0–100 with a breakdown of contributing signals.

**Competitor Radar** — Top patent holders, research institutions, and government buyers per sector, pulled from real contract and patent data.

**White Space Detector** — Finds sectors where innovation signals (patents + research) outpace market signals (contracts + grants). A gap score above 50 is flagged as HIGH opportunity.

**Opportunity Bundle** — Combines all of the above into one API call per sector. Used by both the right panel in Findout and the Intelligence page.

---

## Search

Search runs through a two-stage pipeline:

1. **TF-IDF** — fast keyword scoring across 32,000+ enriched records
2. **Sentence embeddings** — 768-dim semantic vectors (all-mpnet-base-v2) rerank the top candidates

When you type anything — a niche idea, a technology, a problem statement — the query is embedded and matched against the full database. Results come back with a relevance % and a match reason. If the embedding model is still loading on first run, the system falls back to SQL automatically.

When a sector filter is set alongside a free-text query, both are combined into one semantic query string so the embeddings rank against the full intent, not just the keyword.

---

## Infrastructure

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, Vite, Tailwind CSS, DM Sans + Fraunces fonts |
| Backend | FastAPI, Python 3.9+ |
| Database | DuckDB (local, no server needed) |
| Search | scikit-learn TF-IDF + sentence-transformers (all-mpnet-base-v2) |
| AI | Groq (llama-3.3-70b-versatile) — all 5 Groq endpoints |

---

## Improvisation

**Better intelligence factor calculation** — Risk score, white space score, market validator grade, and competitor radar are all computed from raw counts (number of failures, number of contracts, etc.). This works but is blunt. A better approach would weight signals by recency (a failure from 2019 matters less than one from 2023), by funding magnitude (a $50M failed startup is a stronger signal than a $200K one), and by sector size (10 failures in a tiny sector is worse than 10 in a large one). The white space score in particular would benefit from a proper TF-IDF or cosine similarity approach comparing research paper titles against open contract descriptions to find actual semantic gaps rather than just counting signals.

---

## Setup

### 1. Install dependencies

```bash
cd backend
pip install -r requirements.txt
pip install groq
```

### 2. Create your `.env` file

Create a file called `.env` in the `backend/` folder:

```bash
# Required — SAM.gov (free, register at sam.gov/content/dap/sba)
SAM_API_KEY=your_sam_api_key_here

# Required — Groq (free, register at console.groq.com)
GROQ_API_KEY=your_groq_api_key_here

# Optional — PatentsView (free, register at patentsview.org/api/doc)
PATENTSVIEW_API_KEY=your_patentsview_api_key_here

# Optional — OpenAlex (no key needed, but adding email gets faster responses)
OPENALEX_EMAIL=you@example.com

# Optional — PubMed (free, register at ncbi.nlm.nih.gov/account)
NCBI_API_KEY=your_ncbi_api_key_here

# Frontend origin
ALLOWED_ORIGINS=http://localhost:5173
```

Grants.gov, LootDrop, SBIR, and NSF need no API key. The only truly required keys are `SAM_API_KEY` and `GROQ_API_KEY`. Everything else is optional but improves speed and data quality.

### 3. Install frontend dependencies

```bash
cd frontend
npm install
```

---

## Running It

```bash
# Terminal 1 — backend
cd backend && uvicorn main:app --reload --port 8000 --timeout-keep-alive 120

# Terminal 2 — frontend
cd frontend && npm run dev
```

### Fetch data

```bash
cd backend
python3 -m collectors.failure_collector --source lootdrop
python3 -m collectors.failure_collector --source failory
python3 -m collectors.failure_collector --source cbinsights
python3 scripts/ingest.py --source grants
python3 scripts/ingest.py --source sam
python3 scripts/ingest.py --source sbir
python3 scripts/ingest.py --source nsf
```

### Build search indexes

```bash
python3 scripts/build_tfidf.py
python3 scripts/build_embeddings.py
```

The `--timeout-keep-alive 120` flag on uvicorn is important — the embedding model (~420MB) loads into memory on the first search request and can take 30–60 seconds. The flag prevents the worker from timing out during that load. After the first search, all subsequent searches are fast.

---

## Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, Vite, Tailwind CSS |
| Backend | FastAPI, Python 3.9+ |
| Database | DuckDB |
| Search | scikit-learn TF-IDF + sentence-transformers |
| AI | Groq (llama-3.3-70b-versatile) |
