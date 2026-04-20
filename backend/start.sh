#!/bin/bash
set -e

export PYTHONPATH=/app

echo "Running ingest..."
python /app/scripts/ingest.py --source failures --disk
python /app/scripts/ingest.py --source grants --disk
python /app/scripts/ingest.py --source nsf --disk
python /app/scripts/ingest.py --source research --10
python /app/scripts/ingest.py --source sbir --disk

echo "Building TF-IDF..."
python /app/scripts/build_tfidf.py

echo "Starting server..."
uvicorn main:app --host 0.0.0.0 --port $PORT --timeout-keep-alive 120
