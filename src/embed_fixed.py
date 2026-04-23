"""
Embedding pipeline - FIXED version (idempotent).

FIX: Track every processed chunk in DynamoDB. Before embedding, check the
manifest. If the chunk_id is already there AND the content hasn't changed,
skip it.

Key idea:
- chunk_id is the DynamoDB partition key
- content_hash attribute detects when a chunk's text was edited
- writing the manifest entry AFTER successful embedding makes retries safe
  (a crash mid-embedding leaves the chunk unmarked, so next run retries it)
"""

import json
import os
import time
import hashlib
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUT_PATH = os.path.join(DATA_DIR, "embeddings_fixed.json")

TABLE_NAME = os.environ.get("MANIFEST_TABLE", "embedding-manifest")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)


def generate_embedding(text: str) -> list[float]:
    """Fake embedding - same as broken version so we can compare honestly."""
    time.sleep(0.05)
    h = hashlib.sha256(text.encode()).digest()
    return [b / 255.0 for b in h[:8]]


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def is_already_processed(chunk_id: str, current_hash: str) -> bool:
    """Check the DynamoDB manifest. Skip only if chunk_id exists AND hash matches."""
    try:
        resp = table.get_item(Key={"chunk_id": chunk_id})
    except ClientError as e:
        print(f"  DynamoDB error for {chunk_id}: {e}. Treating as not processed.")
        return False

    item = resp.get("Item")
    if item is None:
        return False
    return item.get("content_hash") == current_hash


def mark_processed(chunk_id: str, doc_id: str, current_hash: str) -> None:
    table.put_item(Item={
        "chunk_id": chunk_id,
        "doc_id": doc_id,
        "content_hash": current_hash,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "embedding_dim": 8,
    })


def run_pipeline():
    with open(os.path.join(DATA_DIR, "chunks.json"), "r", encoding="utf-8") as f:
        chunks = json.load(f)

    print(f"Processing {len(chunks)} chunks (idempotent mode)...")
    start = time.time()

    embeddings = []
    skipped = 0
    embedded = 0

    for chunk in chunks:
        current_hash = content_hash(chunk["text"])

        if is_already_processed(chunk["chunk_id"], current_hash):
            skipped += 1
            continue

        vector = generate_embedding(chunk["text"])
        embeddings.append({
            "chunk_id": chunk["chunk_id"],
            "doc_id": chunk["doc_id"],
            "vector": vector,
        })
        mark_processed(chunk["chunk_id"], chunk["doc_id"], current_hash)
        embedded += 1

    # append to the output file if it exists, rather than overwriting
    existing = []
    if os.path.exists(OUT_PATH):
        with open(OUT_PATH, "r", encoding="utf-8") as f:
            existing = json.load(f)
    existing_ids = {e["chunk_id"] for e in existing}
    merged = existing + [e for e in embeddings if e["chunk_id"] not in existing_ids]
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f)

    elapsed = time.time() - start
    print(f"Done in {elapsed:.2f}s")
    print(f"  Embedded: {embedded}  (API calls made)")
    print(f"  Skipped:  {skipped}   (already in manifest)")
    return {"embedded": embedded, "skipped": skipped, "total": len(chunks)}


if __name__ == "__main__":
    run_pipeline()
