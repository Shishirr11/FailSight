# FailSight — Opportunity & Risk Intelligence Platform

One place to see what funding exists, what companies have failed before you,
and whether your idea has a real chance. No paywalls, no signups — all data
pulled from public sources and stored locally.

---

## What's Built

### Data Sources
| Source | Records | What You Get |
|---|---|---|
| Grants.gov | ~thousands | Federal grants with full descriptions, funding amounts, deadlines |
| SAM.gov | ~thousands | Government contracts and solicitations with full text |
| OpenAlex / PubMed | ~thousands | Research papers with abstracts, authors, citations |
| PatentsView | ~thousands | Patents with abstracts, assignees, inventors |
| CB Insights (CSV) | 891 | Failed startups — name, sector, failure reasons |
| Failory | 135 | Failed startups — full articles, founders, exact funding |
| LootDrop | 1,741 | Failed startups — failure analysis, market analysis, lessons, scores |

**2,767 failed startups + thousands of active opportunities in one local database.**

### Pages

**Home** — Browse grants, contracts, patents, and research in tabs. Search in plain English.
Click any item for the full detail. The right panel shows an AI-generated sector briefing
using real data from your database.

**Trial & Errors** — Explore all 2,767 failed startups. Filter by sector, failure reason,
source, or year. Click any company for the full story — what they tried, what went wrong,
what others learned.

**Watchlist** — Save keyword + sector searches. Come back and run them to see what new
opportunities have appeared since you last checked.

### Intelligence Modules
- **Risk Scorer** — LOW / MEDIUM / HIGH based on sector failure history
- **Market Validator** — grades market strength from grant + contract activity
- **Competitor Radar** — top patent holders, research institutions, and government buyers
- **White Space Detector** — finds sectors with high R&D activity but low market contracts
- **Opportunity Bundle** — combines all of the above into one API call per sector

### Infrastructure
- **Database** — DuckDB (local, no server needed)
- **Search indexes** — TF-IDF keyword index + 768-dim sentence embeddings (all-mpnet-base-v2)
- **Pipeline** — `ingest.py` runs all collectors. Can also be triggered from the UI.

---

## What Still Needs to Be Built

### 1. Query Engine:
When a user types an idea, match it against every record in the database using TF-IDF +
embeddings combined. Show results with a relevance % and a reason why it matched.
Needs: `scripts/search/query_engine.py` + `backend/routers/intelligence.py`

### 2. Domain Comparison Panel:
Replace the single risk label with a side-by-side comparison of related sectors showing
match %, average funding, and failure rate. Computed from the local DB — no external calls.

### 3. Replace Search
The current search bar uses Groq (external AI API). Needs to be replaced with the local
query engine so it works without an API key and searches across all data types.

### 4. Updated Home Layout for Ideas
After searching an idea, show matched grants, contracts, patents, research papers, and
failed startups all together — not just one tab at a time.

---
## Setup

### 1. Install dependencies
```bash
cd backend
pip install -r requirements.txt
```

### 2. Create your `.env` file
Create a file called `.env` in the `backend/` folder with the following keys:
```env
# Required — SAM.gov (free, register at sam.gov/content/dap/sba)
SAM_API_KEY=your_sam_api_key_here

# Required — Groq (free, register at console.groq.com)
GROQ_API_KEY=your_groq_api_key_here

# Optional — PatentsView (free, register at patentsview.org/api/doc)
# Without this you still get patents but at lower rate limits
PATENTSVIEW_API_KEY=your_patentsview_api_key_here

# Optional — OpenAlex (no key needed, but adding email gets you faster responses)
OPENALEX_EMAIL=you@example.com

# Optional — PubMed (free, register at ncbi.nlm.nih.gov/account)
# Without this you get 3 requests/sec instead of 10
NCBI_API_KEY=your_ncbi_api_key_here

# Frontend origin
ALLOWED_ORIGINS=http://localhost:5173
```

> Grants.gov and LootDrop need no API key at all.
> The only truly required keys are `SAM_API_KEY` and `GROQ_API_KEY`.
> Everything else is optional but improves speed and data quality.

### 3. Install frontend dependencies
```bash
cd frontend
npm install
```

---

## Running It
```bash
# Terminal 1 — backend
cd backend && uvicorn main:app --reload --port 8000

# Terminal 2 — frontend
cd frontend && npm run dev
```

**Fetch data:**
```bash
cd backend
python3 -m collectors.failure_collector --source lootdrop
python3 -m collectors.failure_collector --source failory
python3 -m collectors.failure_collector --source cbinsights
python3 scripts/ingest.py --source grants
python3 scripts/ingest.py --source sam
```

**Build search indexes:**
```bash
python3 scripts/build_tfidf.py
python3 scripts/build_embeddings.py
```

---

## Stack

| | |
|---|---|
| Frontend | React 18, Vite, Tailwind, Recharts |
| Backend | FastAPI, Python 3.9+ |
| Database | DuckDB |
| Search | scikit-learn TF-IDF + sentence-transformers |
| AI briefings | Groq (llama-3.1-8b) — only for sector briefings |
