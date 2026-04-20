#!/bin/bash
set -e

echo "Running ingest from raw files..."
cd /app/backend
python ../scripts/ingest.py --source failures --disk
python ../scripts/ingest.py --source grants --disk
python ../scripts/ingest.py --source nsf --disk
python ../scripts/ingest.py --source research --disk
python ../scripts/ingest.py --source sbir --disk

echo "Building TF-IDF index..."
python ../scripts/build_tfidf.py

echo "Starting server..."
uvicorn main:app --host 0.0.0.0 --port $PORT --timeout-keep-alive 120