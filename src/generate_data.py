"""
Generate sample document chunks to feed the embedding pipeline.

Writes chunks.json with 10 documents, ~5 chunks each = 50 chunks total.
Each chunk has a stable chunk_id (doc_id + index) so re-runs produce the
same IDs — which is what idempotency depends on.
"""

import json
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

DOCS = [
    ("doc_001", "Introduction to machine learning. Supervised learning uses labeled data. Unsupervised learning finds patterns. Reinforcement learning learns from rewards. Neural networks are inspired by the brain."),
    ("doc_002", "Python basics. Variables store values. Functions group logic. Classes define objects. Modules organize code. Packages bundle modules."),
    ("doc_003", "Cloud computing fundamentals. AWS offers compute. Azure provides services. GCP has machine learning tools. Serverless abstracts servers."),
    ("doc_004", "Database systems overview. SQL uses tables. NoSQL is schemaless. DynamoDB is key-value. MongoDB is document-based. Redis is in-memory."),
    ("doc_005", "Web development stack. HTML structures pages. CSS styles them. JavaScript adds behavior. React builds UIs. Node runs JS on servers."),
    ("doc_006", "DevOps practices. CI builds code. CD deploys it. Docker packages apps. Kubernetes orchestrates containers. Terraform manages infra."),
    ("doc_007", "Data engineering pipelines. Bronze holds raw data. Silver cleans it. Gold is business-ready. Spark processes big data. Airflow schedules jobs."),
    ("doc_008", "LLM applications. Embeddings turn text to vectors. Vector DBs store embeddings. RAG retrieves context. Prompts guide the model. Fine-tuning adapts it."),
    ("doc_009", "Security basics. Authentication verifies identity. Authorization grants access. Encryption protects data. HTTPS secures traffic. IAM manages roles."),
    ("doc_010", "Observability pillars. Logs record events. Metrics measure state. Traces follow requests. Alerts notify on issues. Dashboards visualize data."),
]


def chunk_doc(doc_id: str, text: str) -> list[dict]:
    sentences = [s.strip() for s in text.split(".") if s.strip()]
    return [
        {
            "chunk_id": f"{doc_id}_chunk_{i:03d}",
            "doc_id": doc_id,
            "text": sentence,
        }
        for i, sentence in enumerate(sentences)
    ]


all_chunks = []
for doc_id, text in DOCS:
    all_chunks.extend(chunk_doc(doc_id, text))

out_path = os.path.join(DATA_DIR, "chunks.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(all_chunks, f, indent=2)

print(f"Wrote {len(all_chunks)} chunks from {len(DOCS)} documents to {out_path}")
