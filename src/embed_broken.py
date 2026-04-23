"""
Embedding pipeline - BROKEN version.

PROBLEM: No idempotency. Every run re-processes every chunk.
- Re-run after a crash? Re-embed everything.
- Add 1 new document? Re-embed all 50+ existing chunks too.
- No way to know which chunks were already done.

Cost impact: if embeddings cost $0.0001/chunk, running on 1M chunks twice
costs $200 instead of $100.
"""

import json
import os
import time
import hashlib

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUT_PATH = os.path.join(DATA_DIR, "embeddings_broken.json")


def generate_embedding(text: str) -> list[float]:
    """Fake embedding generation - simulates latency and cost of a real call."""
    time.sleep(0.05)  # pretend the API call takes 50ms
    # deterministic "embedding" from text hash, 8 dims
    h = hashlib.sha256(text.encode()).digest()
    return [b / 255.0 for b in h[:8]]


def run_pipeline():
    with open(os.path.join(DATA_DIR, "chunks.json"), "r", encoding="utf-8") as f:
        chunks = json.load(f)

    print(f"Processing {len(chunks)} chunks...")
    start = time.time()

    embeddings = []
    for chunk in chunks:
        # NO check for "already processed" - every chunk gets re-embedded every run
        vector = generate_embedding(chunk["text"])
        embeddings.append({
            "chunk_id": chunk["chunk_id"],
            "doc_id": chunk["doc_id"],
            "vector": vector,
        })

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(embeddings, f)

    elapsed = time.time() - start
    print(f"Done. Embedded {len(embeddings)} chunks in {elapsed:.2f}s")
    print(f"  (API calls made: {len(embeddings)})")
    return len(embeddings)


if __name__ == "__main__":
    run_pipeline()
