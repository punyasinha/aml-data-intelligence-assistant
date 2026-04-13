"""
query.py — RAG query engine for the Data Intelligence Assistant.

Retrieves relevant dbt model context from ChromaDB, then uses an LLM
to synthesise a grounded answer. Supports both OpenAI and Anthropic Claude.

Usage:
    from query import DataIntelligenceAssistant
    assistant = DataIntelligenceAssistant()
    answer = assistant.ask("Which models depend on stg_customers?")
"""

import os
from pathlib import Path
from typing import Literal

import chromadb
from chromadb.config import Settings
from openai import OpenAI

_HERE = Path(__file__).parent.resolve()
CHROMA_PATH = str(_HERE / "chroma_db")
COLLECTION_NAME = "dbt_knowledge_base"
TOP_K = 5  # number of retrieved chunks per query

SYSTEM_PROMPT = """You are a Data Intelligence Assistant for an AML (Anti-Money Laundering)
data platform built on Snowflake and dbt.

You have access to the full dbt project metadata: model descriptions, column definitions,
data lineage (upstream/downstream dependencies), materialization strategies, tests, and tags.

Your job is to answer questions from data engineers, analysts, and compliance teams about:
- What specific dbt models do and how they work
- Data lineage and model dependencies
- Column definitions and business rules
- AML rule logic (flagging, risk scoring, structuring detection)
- Pipeline failures and troubleshooting guidance
- Data quality tests and their purpose

Guidelines:
- Always cite the specific model name(s) your answer is based on
- If you reference a column, state which model it belongs to
- If the question cannot be answered from the retrieved context, say so clearly
- Be concise and precise — analysts need quick, actionable answers
- When discussing AML rules, be accurate about thresholds and regulatory context
"""


class DataIntelligenceAssistant:

    def __init__(
        self,
        llm_provider: Literal["openai", "anthropic"] = "openai",
        model: str = "gpt-4o-mini",
        api_key: str | None = None,
    ):
        self.llm_provider = llm_provider
        self.model = model

        self.chroma_client = chromadb.PersistentClient(
            path=CHROMA_PATH,
            settings=Settings(anonymized_telemetry=False)
        )
        self.collection = self.chroma_client.get_or_create_collection(COLLECTION_NAME)

        resolved_key = api_key or os.environ.get("OPENAI_API_KEY")
        if llm_provider == "openai":
            self.openai_client = OpenAI(api_key=resolved_key)
        elif llm_provider == "anthropic":
            import anthropic
            self.anthropic_client = anthropic.Anthropic(
                api_key=api_key or os.environ.get("ANTHROPIC_API_KEY")
            )

    def _embed_query(self, query: str) -> list[float]:
        response = self.openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=query
        )
        return response.data[0].embedding

    def _retrieve(self, query: str) -> list[dict]:
        """Retrieve top-K relevant model documents from ChromaDB."""
        query_embedding = self._embed_query(query)
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=TOP_K,
            include=["documents", "metadatas", "distances"]
        )

        chunks = []
        for i, doc in enumerate(results["documents"][0]):
            chunks.append({
                "text": doc,
                "metadata": results["metadatas"][0][i],
                "distance": results["distances"][0][i],
            })
        return chunks

    def _build_context(self, chunks: list[dict]) -> str:
        """Format retrieved chunks into a context block for the LLM."""
        parts = []
        for i, chunk in enumerate(chunks, 1):
            model_name = chunk["metadata"].get("model_name", "unknown")
            parts.append(f"[Context {i} — {model_name}]\n{chunk['text']}")
        return "\n\n---\n\n".join(parts)

    def _call_openai(self, context: str, question: str) -> str:
        response = self.openai_client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Use the following dbt project context to answer the question.\n\n"
                        f"=== CONTEXT ===\n{context}\n\n"
                        f"=== QUESTION ===\n{question}"
                    )
                }
            ],
            temperature=0.1,
        )
        return response.choices[0].message.content

    def _call_anthropic(self, context: str, question: str) -> str:
        message = self.anthropic_client.messages.create(
            model=self.model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Use the following dbt project context to answer the question.\n\n"
                        f"=== CONTEXT ===\n{context}\n\n"
                        f"=== QUESTION ===\n{question}"
                    )
                }
            ]
        )
        return message.content[0].text

    def ask(self, question: str) -> dict:
        """
        Ask a question about the dbt project.

        Returns:
            dict with keys: answer (str), sources (list[dict]), question (str)
        """
        chunks = self._retrieve(question)
        context = self._build_context(chunks)

        if self.llm_provider == "openai":
            answer = self._call_openai(context, question)
        elif self.llm_provider == "anthropic":
            answer = self._call_anthropic(context, question)
        else:
            raise ValueError(f"Unknown LLM provider: {self.llm_provider}")

        return {
            "question": question,
            "answer": answer,
            "sources": [
                {
                    "model": c["metadata"].get("model_name"),
                    "type": c["metadata"].get("resource_type"),
                    "schema": c["metadata"].get("schema"),
                    "file": c["metadata"].get("file_path"),
                    "relevance_score": round(1 - c["distance"], 3),
                }
                for c in chunks
            ]
        }


if __name__ == "__main__":
    # Quick smoke test
    assistant = DataIntelligenceAssistant()

    sample_questions = [
        "Which models depend on stg_customers?",
        "What does the composite_risk_score measure and how is it calculated?",
        "What AML rules are implemented and what thresholds do they use?",
        "How does flag_structuring work?",
        "What columns are available in fct_aml_alerts?",
        "Which customers have been escalated to the FIU?",
    ]

    for q in sample_questions[:2]:
        print(f"\nQ: {q}")
        result = assistant.ask(q)
        print(f"A: {result['answer']}")
        print(f"Sources: {[s['model'] for s in result['sources']]}")
