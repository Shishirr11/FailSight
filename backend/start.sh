#!/bin/bash
set -e

BACKEND_DIR="$(cd "$(dirname "$0")" && pwd)"

export PYTHONPATH="$BACKEND_DIR"

mkdir -p /tmp/failsight/parquet
mkdir -p /tmp/failsight/index
mkdir -p "$BACKEND_DIR/data/search_index"
mkdir -p "$BACKEND_DIR/data/parquet"

echo "BACKEND_DIR: $BACKEND_DIR"
echo "PYTHONPATH:  $PYTHONPATH"

if [ -n "$R2_ACCOUNT_ID" ] && [ -n "$R2_ACCESS_KEY_ID" ] && [ -n "$R2_SECRET_ACCESS_KEY" ]; then
    echo ""
    echo "R2 configured — checking for existing data in R2..."

    python3 - <<PYEOF
import sys
sys.path.insert(0, "$BACKEND_DIR")
from pathlib import Path
from storage.r2_store import download_data_assets, R2CapExceeded, R2Unavailable, key_exists, KEYS

TMP_PARQUET = Path("/tmp/failsight/parquet")
TMP_INDEX   = Path("/tmp/failsight/index")

has_data = False
try:
    has_data = key_exists(KEYS["opportunities_parquet"])
except Exception:
    pass

if has_data:
    print("Found existing data in R2 — downloading...")
    try:
        result = download_data_assets(TMP_PARQUET, TMP_INDEX, skip_embeddings=True)
        print(f"Downloaded: {len(result['downloaded'])} files")
        print(f"Missing:    {len(result['missing'])} files")
    except R2CapExceeded as e:
        print(f"ERROR: R2 cap exceeded — {e}", file=sys.stderr)
    except R2Unavailable as e:
        print(f"WARNING: R2 unavailable — {e}", file=sys.stderr)
    except Exception as e:
        print(f"WARNING: R2 download failed — {e}", file=sys.stderr)
else:
    print("No existing data in R2 — will seed on startup.")
    Path("/tmp/failsight/needs_seed").touch()
PYEOF

else
    echo "R2 not configured — local mode."
    touch /tmp/failsight/needs_seed
fi

if [ -f "/tmp/failsight/needs_seed" ]; then
    echo ""
    echo "Seeding database from repo files + live fetch..."

    cd "$BACKEND_DIR"

    echo "  Loading failures from disk..."
    python3 scripts/ingest.py --source failures --disk || true

    if [ -f "$BACKEND_DIR/data/raw/sbir/award_data.csv" ]; then
        echo "  Loading SBIR from disk..."
        python3 scripts/ingest.py --source sbir --disk || true
    else
        echo "  Fetching SBIR live..."
        python3 scripts/ingest.py --source sbir || true
    fi

    echo "  Fetching grants live..."
    python3 scripts/ingest.py --source grants || true

    echo "  Fetching NSF live..."
    python3 scripts/ingest.py --source nsf || true

    echo "  Building TF-IDF search index..."
    python3 scripts/build_tfidf.py || true

    if [ -n "$R2_ACCOUNT_ID" ]; then
        echo "  Exporting parquet and uploading to R2..."
        python3 - <<PYEOF
import sys
sys.path.insert(0, "$BACKEND_DIR")
from pathlib import Path
from storage.db import get_db, export_parquet
from storage.r2_store import upload_data_assets, R2CapExceeded

TMP_PARQUET = Path("/tmp/failsight/parquet")
INDEX_DIR   = Path("$BACKEND_DIR/data/search_index")

try:
    con = get_db()
    count = con.execute("SELECT COUNT(*) FROM unified_opportunities").fetchone()[0]
    print(f"  DB has {count:,} opportunities — exporting...")
    export_parquet(con, TMP_PARQUET)
    result = upload_data_assets(TMP_PARQUET, INDEX_DIR)
    print(f"  Uploaded {len(result['uploaded'])} files to R2.")
except R2CapExceeded as e:
    print(f"  WARNING: R2 cap hit during seed upload: {e}", file=sys.stderr)
except Exception as e:
    print(f"  WARNING: Upload failed (non-fatal): {e}", file=sys.stderr)
PYEOF
    fi

    rm -f /tmp/failsight/needs_seed
    echo "Seeding complete."
fi

cd "$BACKEND_DIR"
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}" --timeout-keep-alive 120