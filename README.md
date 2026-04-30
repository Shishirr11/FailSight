# Failsight — Founder Intelligence Platform
Live at [fail-sight.vercel.app](https://fail-sight.vercel.app)

One place to see what funding exists, what companies failed before you, and whether your idea has a real shot. No paywalls, no signups. All data is pulled from public sources and stored in Cloudflare R2.

---

## The Trade Off

**Search Index Size**

The TF-IDF matrix at full settings (max_features=50,000, ngram_range=(1,2)) builds to 6.6GB too large for any affordable host. In production it runs at max_features=20,000, ngram_range=(1,2), which produces a 12MB sparse matrix that fits comfortably in 512MB RAM. Sentence embeddings are disabled in production the all-mpnet-base-v2 model loads ~420MB on first request, which kills free-tier instances.

---

## Data Sources

| Source | Records | Notes |
|--------|---------|-------|
| Grants.gov | 1,770 | Federal grants with descriptions, funding, deadlines |
| SBIR.gov | 22,172 | Small business R&D awards with abstracts |
| NSF Awards | 2,703 | National Science Foundation grants with PI details |
| OpenAlex + PubMed | 3,572 | Research papers with abstracts, authors, citations |
| CB Insights (CSV) | 487 | Failed startups, sector, failure reasons |
| Failory | 280 | Failed startups with full articles and funding |
| LootDrop | 2,000 | Failure analysis, market scores, key lessons |
| SAM.gov | pending | Government migrating to new domain |
| PatentsView | pending | Government migrating to new domain |

**2,767 failed startups and 30,000+ active opportunities in one database.**

---

## Storage

All data lives in Cloudflare R2, not on disk. On every cold start, the backend downloads parquet files from R2 into `/tmp/failsight/` and loads them into an in-memory DuckDB instance. After any pipeline run that adds new records, it re-exports parquet and uploads back to R2 so nothing is lost on restart.

Current R2 usage: 81MB of the 8GB free cap.

---

## Pages

**Findout** — Browse grants, contracts, patents, research, and SBIR awards. Search in plain English using TF-IDF across 32,000+ enriched records. Click any card for the full detail view with source links and a Groq generated explanation of why it matches your search. The right panel shows live sector intelligence. Both panels are resizable by dragging the divider.

**Intelligence** — Type your startup idea and get a structured verdict. Idea validation loads first (market exists, confidence level, biggest risk, first grant to apply for). Gap Finder and Grant Match expand below as collapsible sections. All answers come from Groq reading your actual database, not the model's training data.

**Idea Graveyard** — 2,767 documented startup failures. Filter by sector, failure reason, year range, and funding raised. Click any company for the full post mortem. Text from data sources is cleaned before display.

---

## AI Features

All five Groq endpoints read from the live database before generating anything.

**Why This Matters** — When you open a detail card after a search, Groq writes sentences explaining exactly why that specific result is relevant to what you searched for.

**Idea Validator** — Takes your startup description, searches the grants table and failure post-mortems, returns: market exists (yes/no), confidence level, a verdict, the biggest risk, and the single best grant to apply for first.

**Gap Finder** — For a sector, reads white-space signals, recent research titles, open SAM contracts, and failure reasons. Returns a 2x2 grid: tried and failed / researched but not funded / open demand / non-obvious play.

**Grant Match** — Paste your project description. Groq compares it against the top 5 matching grants and returns a fit score (1-10), what you already meet, what's missing, and a ranked list of all matches.

**Sector Suggest** — As you type in the sector filter, Groq returns the most relevant sectors in real time. Falls back to substring match if Groq is unavailable.

---

## Intelligence Modules

**Risk Scorer** — LOW / MEDIUM / HIGH based on sector failure history and top failure reasons.

**Market Validator** — Grades market strength A-D from grant and contract activity, patent density, and research volume. Scores 0-100 with a signal breakdown.

**Competitor Radar** — Top patent holders, research institutions, and government buyers per sector, pulled from real contract and patent data.

**White Space Detector** — Finds sectors where innovation signals (patents and research) outpace market signals (contracts and grants). A gap score above 50 is flagged HIGH.

**Opportunity Bundle** — Combines all of the above in one API call per sector. Powers both the right panel in Findout and the sector briefings on the Intelligence page.

---

## Search

Search runs through a two-stage pipeline. TF-IDF scores 32,000+ enriched records fast. When source tabs are used during a search (Grants, SAM, Research, etc.), the filter is applied by pre-filtering the ranked TF-IDF IDs against the database before pagination, so you always get the right source, not just whatever happened to rank highest across all sources.

If the TF-IDF index isn't loaded, search falls back to a SQL keyword query automatically.

---

## Infrastructure

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, Vite, Tailwind CSS, DM Sans + Fraunces |
| Backend | FastAPI, Python 3.11 |
| Database | DuckDB in-memory, loaded from R2 on startup |
| Storage | Cloudflare R2 (parquet + TF-IDF index) |
| Search | scikit-learn TF-IDF, sparse .npz matrix |
| AI | Groq llama-3.3-70b-versatile |
| Hosting | Render (backend) + Vercel (frontend) |

---

## Setup

### 1. Install dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Create your `.env` file in `backend/`

```bash
GROQ_API_KEY=your_groq_api_key
SAM_API_KEY=your_sam_api_key
NCBI_API_KEY=your_ncbi_api_key
OPENALEX_API_KEY=your_openalex_api_key
OPENALEX_EMAIL=you@example.com
ALLOWED_ORIGINS=http://localhost:5173

R2_ACCOUNT_ID=your_account_id
R2_ACCESS_KEY_ID=your_access_key
R2_SECRET_ACCESS_KEY=your_secret_key
R2_BUCKET_NAME=failsight-data
```

GROQ_API_KEY is the only key you truly need to run locally. Everything else is optional but improves data coverage.

### 3. Install frontend dependencies

```bash
cd frontend
npm install
```

---

## Running It

```bash

cd backend && uvicorn main:app --reload --port 8000 --timeout-keep-alive 120

# Terminal 2 — frontend
cd frontend && npm run dev
```

### Fetch data

```bash
cd backend
python3 scripts/ingest.py --source failures --disk
python3 scripts/ingest.py --source sbir
python3 scripts/ingest.py --source grants
python3 scripts/ingest.py --source nsf
python3 scripts/ingest.py --source research
```

### Build search index

```bash
python3 scripts/build_tfidf.py
```

---

## Deployment

Backend runs on Render. On startup, `start.sh` checks R2 for existing parquet data. If found, it downloads and loads it directly. If not (first deploy), it seeds from the committed CSV files, fetches live data, builds the TF-IDF index, and uploads everything to R2. Cold starts take 4-6 minutes because of this.

Frontend runs on Vercel. Set `VITE_API_BASE` to your Render backend URL in Vercel's environment variables and redeploy.

Render's free tier spins down after 15 minutes of inactivity. The next request after sleep takes while R2 data reloads.

---

## What's Not Working Yet

SAM.gov and PatentsView are both mid-migration to new government domains. Their APIs are returning errors or redirecting. Both collectors exist in the codebase and will work once the new endpoints stabilise.

Sentence embeddings are disabled in production. The index files exist locally but the all-mpnet-base-v2 model is too heavy for the free Render tier. TF-IDF handles search well enough at this scale.