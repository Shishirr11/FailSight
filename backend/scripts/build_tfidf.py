import sys
import json
import pickle
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from storage.db import get_db
from loguru import logger
from tqdm import tqdm

from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

INDEX_DIR     = Path(__file__).resolve().parent.parent / "data" / "search_index"
VECTORIZER_PATH = INDEX_DIR / "tfidf_vectorizer.pkl"
MATRIX_PATH     = INDEX_DIR / "tfidf_matrix.npz"
IDS_PATH        = INDEX_DIR / "tfidf_record_ids.json"

def build(batch_size: int = 500):
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    con = get_db()

    logger.info("Loading enriched records from DB...")
    records = con.execute("""
        SELECT record_id, source, record_type, full_text, summary
        FROM enriched_details
        WHERE enrichment_status = 'done'
          AND (full_text IS NOT NULL AND full_text != '')
        ORDER BY record_id
    """).fetchall()

    if not records:
        logger.error("No enriched records found. Run enrichers first.")
        return

    logger.info(f"Building TF-IDF index over {len(records):,} records...")

    record_ids = []
    corpus     = []

    for record_id, source, record_type, full_text, summary in records:

        text = f"{summary or ''} {summary or ''} {full_text or ''}"
        record_ids.append(record_id)
        corpus.append(text)

    logger.info("Fitting TF-IDF vectorizer (this may take 30-60 seconds)...")
    # vectorizer = TfidfVectorizer(
    #     sublinear_tf=True,
    #     min_df=2,
    #     max_df=0.95,
    #     max_features=50_000,
    #     ngram_range=(1, 2),
    #     strip_accents="unicode",
    #     analyzer="word",
    # )
    vectorizer = TfidfVectorizer(
        lowercase=True,
        strip_accents="unicode",
        analyzer="word",
        stop_words="english",
        token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z0-9\-]+\b",
        sublinear_tf=True,
        min_df=2,
        max_df=0.90,
        max_features=20_000,
        ngram_range=(1, 2),
        norm="l2",
        dtype=np.float32,
    )
    matrix = vectorizer.fit_transform(corpus)
    logger.success(f"Vectorizer fitted — vocab size: {len(vectorizer.vocabulary_):,}")
    logger.info(f"Matrix shape: {matrix.shape} (records × terms)")

    with open(VECTORIZER_PATH, "wb") as f:
        pickle.dump(vectorizer, f)
    from scipy.sparse import save_npz
    save_npz(str(MATRIX_PATH).replace('.npy', '.npz'), matrix.astype(np.float32))
    IDS_PATH.write_text(json.dumps(record_ids))

    logger.success(f"Index saved to {INDEX_DIR}/")
    logger.info(f"  vectorizer: {VECTORIZER_PATH.name}")
    logger.info(f"  matrix:     {MATRIX_PATH.name}  ({matrix.shape[0]:,} × {matrix.shape[1]:,})")
    logger.info(f"  ids:        {IDS_PATH.name}")

    logger.info("Writing sparse vectors back to DB...")
    feature_names = vectorizer.get_feature_names_out()
    dense         = matrix.toarray()

    batch_updates = []
    for i, (record_id, row) in enumerate(tqdm(zip(record_ids, dense), total=len(record_ids), desc="Storing vectors")):

        top_idx    = np.argsort(row)[::-1][:50]
        sparse_vec = {feature_names[j]: float(row[j]) for j in top_idx if row[j] > 0}
        batch_updates.append((json.dumps(sparse_vec), record_id))

        if len(batch_updates) >= batch_size:
            con.executemany(
                "UPDATE enriched_details SET tfidf_vector = ? WHERE record_id = ?",
                batch_updates
            )
            batch_updates = []

    if batch_updates:
        con.executemany(
            "UPDATE enriched_details SET tfidf_vector = ? WHERE record_id = ?",
            batch_updates
        )

    logger.success("TF-IDF index build complete.")

    vocab_sample = list(vectorizer.vocabulary_.keys())[:20]
    logger.info(f"Sample vocabulary: {vocab_sample}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    build(batch_size=args.batch_size)