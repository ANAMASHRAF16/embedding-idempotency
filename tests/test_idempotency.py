"""
Idempotency test.

Runs the fixed pipeline twice and asserts:
  - First run embeds everything (embedded == total, skipped == 0)
  - Second run skips everything (embedded == 0, skipped == total)

Requires a real DynamoDB table (or a clean one) — use the MANIFEST_TABLE
env var to point at a test-specific table if you don't want to pollute prod.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from embed_fixed import run_pipeline, table


def clear_manifest():
    """Wipe the table so the first run starts clean."""
    print("Clearing manifest for test run...")
    scan = table.scan(ProjectionExpression="chunk_id")
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"chunk_id": item["chunk_id"]})


def main():
    clear_manifest()

    print("\n=== First run ===")
    first = run_pipeline()
    assert first["skipped"] == 0, f"First run should skip nothing, got {first['skipped']}"
    assert first["embedded"] == first["total"], "First run should embed every chunk"

    print("\n=== Second run ===")
    second = run_pipeline()
    assert second["embedded"] == 0, f"Second run should embed nothing, got {second['embedded']}"
    assert second["skipped"] == second["total"], "Second run should skip every chunk"

    print("\nPASS — pipeline is idempotent.")


if __name__ == "__main__":
    main()
