import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from storage.db import get_db
from loguru import logger
from tqdm import tqdm
import numpy as np

INDEX_DIR   = Path(__file__).resolve().parent.parent / "data" / "search_index"
EMBED_PATH  = INDEX_DIR / "embeddings_matrix.npy"
IDS_PATH    = INDEX_DIR / "embedding_record_ids.json"

def build(model_name: str = "all-mpnet-base-v2", batch_size: int = 32):
    INDEX_DIR.mkdir(parents=True, exist_ok=True)

    from sentence_transformers import SentenceTransformer
    import torch

    if torch.backends.mps.is_available():
        device = "mps"
        logger.info("got the MPS ")
    elif torch.cuda.is_available():
        device = "cuda"
    else:
        device = "cpu"
        logger.info("Using MiniLM.")

    logger.info(f"Loading model: {model_name}")
    model = SentenceTransformer(model_name, device=device)
    dim   = model.get_sentence_embedding_dimension()
    logger.info(f"Model loaded — embedding dimension: {dim}")

    con = get_db()

    records = con.execute("""
        SELECT record_id, summary, full_text
        FROM enriched_details
        WHERE enrichment_status = 'done'
          AND embedding IS NULL
          AND (full_text IS NOT NULL AND full_text != '')
        ORDER BY record_id
    """).fetchall()

    if not records:
        logger.info("All records already have embeddings.")
        return

    logger.info(f"Generating embeddings for {len(records):,} records (batch_size={batch_size})...")

    record_ids  = []
    all_embeddings = []

    for i in tqdm(range(0, len(records), batch_size), desc="Embedding batches"):
        batch = records[i : i + batch_size]

        texts = []
        ids   = []
        for record_id, summary, full_text in batch:

            text = (summary or full_text or "")[:2000]
            texts.append(text)
            ids.append(record_id)

        embeddings = model.encode(
            texts,
            batch_size        = batch_size,
            show_progress_bar = False,
            normalize_embeddings = True,   

            convert_to_numpy  = True,
        )

        updates = []
        for record_id, emb in zip(ids, embeddings):

            updates.append((emb.tolist(), record_id))
            record_ids.append(record_id)
            all_embeddings.append(emb)

        con.executemany(
            "UPDATE enriched_details SET embedding = ? WHERE record_id = ?",
            updates
        )

    if all_embeddings:
        matrix = np.array(all_embeddings, dtype=np.float32)
        np.save(EMBED_PATH, matrix)
        IDS_PATH.write_text(json.dumps(record_ids))
        logger.success(f"Embeddings matrix saved → {EMBED_PATH} ({matrix.shape})")

    logger.success(f"Embedding generation complete — {len(record_ids):,} records embedded.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model",      default="all-mpnet-base-v2",
                        help="Sentence transformer model name")
    parser.add_argument("--batch-size", type=int, default=32)
    args = parser.parse_args()
    build(model_name=args.model, batch_size=args.batch_size)