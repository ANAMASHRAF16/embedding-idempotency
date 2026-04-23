# Embedding Idempotency

An embedding pipeline that's safe to re-run — it tracks processed chunks in DynamoDB and skips them on subsequent runs.

## Problem

The baseline pipeline has no memory of prior runs:
- Crashed halfway through 1M chunks? Start over from zero.
- Added one new doc? Re-embed everything again.
- Paying for every embedding API call on every run.

## Fix

| Change | Before | After |
|---|---|---|
| Processed tracking | None | DynamoDB `embedding-manifest` table keyed by `chunk_id` |
| Re-run cost | Same as first run | Near-zero (only new/changed chunks) |
| Crash recovery | Start over | Resume from last processed chunk |
| Detecting content changes | Not possible | `content_hash` attribute — re-embeds if text changed |

## Architecture

```
chunks.json ─> for each chunk:
                 1. hash the text
                 2. DynamoDB.get_item(chunk_id) → already processed?
                 3a. YES + same hash → SKIP
                 3b. NO, or hash changed → embed + write manifest entry
```

## AWS Setup

Create a DynamoDB table:
- **Name:** `embedding-manifest`
- **Partition key:** `chunk_id` (String)
- **Billing:** On-demand

Give your IAM user `AmazonDynamoDBFullAccess` and run `aws configure` locally.

## Run

```bash
pip install -r requirements.txt

# Generate 50 sample chunks
python src/generate_data.py

# Run broken version — every run does the same work
python src/embed_broken.py
python src/embed_broken.py   # same cost/time again

# Run fixed version — second run is near-instant
python src/embed_fixed.py
python src/embed_fixed.py    # all chunks skipped

# Test idempotency
python tests/test_idempotency.py
```
