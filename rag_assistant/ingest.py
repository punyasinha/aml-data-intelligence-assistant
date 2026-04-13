"""
ingest.py — Parse dbt artifacts and load into ChromaDB vector store.

Reads dbt's generated manifest.json and catalog.json from the dbt target/
directory, extracts model metadata (descriptions, columns, tests, lineage),
and embeds each chunk into a persistent ChromaDB collection.

Run this after: dbt docs generate

Usage:
    python ingest.py
    python ingest.py --manifest ../dbt_project/target/manifest.json
"""

import json
import os
import argparse
from pathlib import Path

import chromadb
from chromadb.config import Settings
from openai import OpenAI

_HERE = Path(__file__).parent.resolve()
CHROMA_PATH = str(_HERE / "chroma_db")
COLLECTION_NAME = "dbt_knowledge_base"
# Manifest paths — resolved relative to this file's location
MANIFEST_PATH = str(_HERE.parent / "dbt_project" / "target" / "manifest.json")
CATALOG_PATH = str(_HERE.parent / "dbt_project" / "target" / "catalog.json")


def is_ingested() -> bool:
    """Return True if ChromaDB already has documents loaded."""
    try:
        client = chromadb.PersistentClient(
            path=CHROMA_PATH,
            settings=Settings(anonymized_telemetry=False)
        )
        col = client.get_collection(COLLECTION_NAME)
        return col.count() > 0
    except Exception:
        return False


def load_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def extract_model_documents(manifest: dict, catalog: dict | None) -> list[dict]:
    """
    Convert dbt manifest nodes into embeddable text documents.
    Each document represents one dbt model with its full metadata.
    """
    documents = []
    nodes = manifest.get("nodes", {})

    for node_id, node in nodes.items():
        if node.get("resource_type") not in ("model", "seed", "test"):
            continue

        model_name = node.get("name", "")
        description = node.get("description", "No description provided.")
        schema = node.get("schema", "")
        database = node.get("database", "")
        materialized = node.get("config", {}).get("materialized", "unknown")
        tags = ", ".join(node.get("tags", []))
        path = node.get("original_file_path", "")

        # Column descriptions
        columns = node.get("columns", {})
        col_lines = []
        for col_name, col_meta in columns.items():
            col_desc = col_meta.get("description", "")
            col_tests = ", ".join(
                [t if isinstance(t, str) else list(t.keys())[0]
                 for t in col_meta.get("tests", [])]
            )
            line = f"  - {col_name}: {col_desc}"
            if col_tests:
                line += f" [tests: {col_tests}]"
            col_lines.append(line)

        # Upstream dependencies
        depends_on = node.get("depends_on", {}).get("nodes", [])
        deps_clean = [d.split(".")[-1] for d in depends_on]

        # Catalog-enriched row count (if available)
        catalog_stats = ""
        if catalog:
            cat_nodes = catalog.get("nodes", {})
            if node_id in cat_nodes:
                stats = cat_nodes[node_id].get("stats", {})
                row_count = stats.get("row_count", {}).get("value", "unknown")
                catalog_stats = f"\nRow count (last run): {row_count}"

        # Build the full text chunk for this model
        text = f"""
Model: {model_name}
Type: {node.get('resource_type', 'model')}
Materialization: {materialized}
Schema: {database}.{schema}
File: {path}
Tags: {tags}
Description: {description}
Dependencies (upstream models): {', '.join(deps_clean) if deps_clean else 'none'}
{catalog_stats}

Columns:
{chr(10).join(col_lines) if col_lines else '  (no column documentation)'}
""".strip()

        documents.append({
            "id": node_id,
            "text": text,
            "metadata": {
                "model_name": model_name,
                "resource_type": node.get("resource_type", "model"),
                "schema": schema,
                "materialized": materialized,
                "tags": tags,
                "file_path": path,
            }
        })

    return documents


def extract_source_documents(manifest: dict) -> list[dict]:
    """Extract dbt source definitions as searchable documents."""
    documents = []
    sources = manifest.get("sources", {})

    for source_id, source in sources.items():
        name = source.get("name", "")
        source_name = source.get("source_name", "")
        description = source.get("description", "No description provided.")
        schema = source.get("schema", "")
        database = source.get("database", "")

        columns = source.get("columns", {})
        col_lines = [
            f"  - {col}: {meta.get('description', '')}"
            for col, meta in columns.items()
        ]

        text = f"""
Source Table: {source_name}.{name}
Database.Schema: {database}.{schema}
Description: {description}

Columns:
{chr(10).join(col_lines) if col_lines else '  (no column documentation)'}
""".strip()

        documents.append({
            "id": source_id,
            "text": text,
            "metadata": {
                "model_name": f"{source_name}.{name}",
                "resource_type": "source",
                "schema": schema,
                "materialized": "external",
                "tags": "source",
                "file_path": source.get("original_file_path", ""),
            }
        })

    return documents


def embed_documents(documents: list[dict], client: OpenAI) -> list[list[float]]:
    """Batch embed document texts using OpenAI text-embedding-3-small."""
    texts = [doc["text"] for doc in documents]
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=texts
    )
    return [item.embedding for item in response.data]


def build_vector_store(documents: list[dict], embeddings: list[list[float]]) -> chromadb.Collection:
    """Persist documents and embeddings into ChromaDB."""
    chroma_client = chromadb.PersistentClient(
        path=CHROMA_PATH,
        settings=Settings(anonymized_telemetry=False)
    )

    # Drop and recreate collection for a clean rebuild
    try:
        chroma_client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass

    collection = chroma_client.create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"}
    )

    collection.add(
        ids=[doc["id"] for doc in documents],
        embeddings=embeddings,
        documents=[doc["text"] for doc in documents],
        metadatas=[doc["metadata"] for doc in documents],
    )

    return collection


def run_ingest(
    manifest_path: str = MANIFEST_PATH,
    catalog_path: str = CATALOG_PATH,
    api_key: str | None = None,
) -> int:
    """
    Run the full ingest pipeline. Returns the number of documents loaded.
    Can be called from app.py on cold start, or from CLI via main().
    """
    openai_client = OpenAI(api_key=api_key or os.environ["OPENAI_API_KEY"])

    if not Path(manifest_path).exists():
        raise FileNotFoundError(
            f"manifest.json not found at {manifest_path}. "
            "Run `dbt docs generate` in the dbt_project directory first, "
            "then commit target/manifest.json to the repository."
        )

    manifest = load_json(manifest_path)

    catalog = None
    if Path(catalog_path).exists():
        catalog = load_json(catalog_path)

    model_docs = extract_model_documents(manifest, catalog)
    source_docs = extract_source_documents(manifest)
    all_docs = model_docs + source_docs

    embeddings = embed_documents(all_docs, openai_client)
    collection = build_vector_store(all_docs, embeddings)
    return collection.count()


def main():
    parser = argparse.ArgumentParser(description="Ingest dbt artifacts into ChromaDB")
    parser.add_argument("--manifest", default=MANIFEST_PATH)
    parser.add_argument("--catalog", default=CATALOG_PATH)
    args = parser.parse_args()

    print("Embedding dbt project metadata into ChromaDB...")
    count = run_ingest(manifest_path=args.manifest, catalog_path=args.catalog)
    print(f"Done. {count} documents loaded into ChromaDB.")
    print("Run the Streamlit app: streamlit run app.py")


if __name__ == "__main__":
    main()
