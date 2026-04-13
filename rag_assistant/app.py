"""
app.py — AML Data Intelligence Assistant
Streamlit app deployed on Streamlit Community Cloud.

Architecture:
- Data platform: Snowflake + dbt (runs locally / in CI)
- Metadata: dbt manifest.json committed to GitHub
- Vector store: ChromaDB built on cold start from manifest.json
- LLM: OpenAI GPT-4o-mini (key stored as Streamlit secret)
- Hosting: Streamlit Community Cloud (public URL)
"""

import os
import streamlit as st

# ── Resolve API key (Streamlit Cloud secret or local env var) ─────────────────
def get_api_key() -> str:
    # Streamlit Cloud stores secrets in st.secrets — may not exist locally
    try:
        if "OPENAI_API_KEY" in st.secrets:
            return st.secrets["OPENAI_API_KEY"]
    except Exception:
        pass
    if "OPENAI_API_KEY" in os.environ:
        return os.environ["OPENAI_API_KEY"]
    st.error(
        "OPENAI_API_KEY not set. "
        "Add it to your shell environment locally (`export OPENAI_API_KEY=...`), "
        "or to Streamlit Cloud secrets in the dashboard."
    )
    st.stop()


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AML Data Intelligence Assistant",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Auto-ingest on cold start ─────────────────────────────────────────────────
# Runs once per deployment. Builds ChromaDB from manifest.json in the repo.
# Takes ~5-10 seconds. Subsequent requests use the cached collection.

@st.cache_resource(show_spinner=False)
def initialise_assistant():
    from ingest import is_ingested, run_ingest, MANIFEST_PATH, CATALOG_PATH
    from query import DataIntelligenceAssistant

    api_key = get_api_key()

    if not is_ingested():
        with st.spinner("Building knowledge base from dbt project metadata..."):
            count = run_ingest(
                manifest_path=MANIFEST_PATH,
                catalog_path=CATALOG_PATH,
                api_key=api_key,
            )
            st.toast(f"Knowledge base ready — {count} model documents loaded.", icon="✅")

    return DataIntelligenceAssistant(
        llm_provider="openai",
        model="gpt-4o-mini",
        api_key=api_key,
    )


assistant = initialise_assistant()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("AML Data Intelligence Assistant")

    st.markdown("""
**Built by [Punya Saryar](https://linkedin.com/in/punya-saryar)**
Senior Data Engineer | Melbourne, AU

---

### About this project

This assistant is powered by a **production-grade AML data platform** built on:

| Layer | Technology |
|---|---|
| Data warehouse | Snowflake |
| Transformation | dbt (Kimball) |
| Orchestration | Apache Airflow |
| Governance | Unity Catalog |
| AI layer | RAG + GPT-4o-mini |
| Vector store | ChromaDB |

The platform models **AML transaction monitoring** — customer risk scoring,
structuring detection, high-risk country flagging, and FIU escalation tracking.

This assistant answers questions about the data models, business logic,
lineage, and AML rules using the real dbt project metadata.

---

### Try asking
""")

    example_questions = [
        "What models exist in this project?",
        "How is the composite risk score calculated?",
        "What AML rules are implemented?",
        "What is flag_structuring and what threshold does it use?",
        "Which models feed into fct_aml_alerts?",
        "What columns does dim_customer have?",
        "How are FIU escalations tracked?",
        "What is the difference between OPEN and ESCALATED alerts?",
        "How does the AUSTRAC large cash threshold work?",
        "What dbt tests are applied to fct_transactions?",
    ]

    for q in example_questions:
        if st.button(q, use_container_width=True, key=f"btn_{q[:20]}"):
            st.session_state["prefill_question"] = q

    st.divider()
    if st.button("Clear conversation", use_container_width=True):
        st.session_state["messages"] = []
        st.rerun()

    st.caption("Powered by dbt + Snowflake + OpenAI + ChromaDB")

# ── Session state ─────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# ── Main area ─────────────────────────────────────────────────────────────────
st.header("AML Data Intelligence Assistant")
st.caption(
    "Ask questions about the AML data platform — model lineage, column definitions, "
    "AML rule logic, risk scoring, and pipeline behaviour."
)

# Hero stats row
col1, col2, col3, col4 = st.columns(4)
col1.metric("dbt Models", "9", "Staging → Marts")
col2.metric("AML Rules", "5", "AUSTRAC-aligned")
col3.metric("Kimball Dims", "2", "Customer + Date")
col4.metric("Fact Tables", "2", "Transactions + Alerts")

st.divider()

# Render conversation
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("sources"):
            with st.expander("Sources used from dbt project", expanded=False):
                for src in msg["sources"]:
                    score = src.get("relevance_score", 0)
                    st.markdown(
                        f"- **`{src['model']}`** ({src['type']}) "
                        f"schema: `{src['schema']}` "
                        f"relevance: `{score}`"
                    )

# Input
prefill = st.session_state.pop("prefill_question", "") if "prefill_question" in st.session_state else ""
question = st.chat_input("Ask about the AML data platform...")

if not question and prefill:
    question = prefill

if question:
    st.session_state["messages"].append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Searching dbt knowledge base..."):
            try:
                result = assistant.ask(question)
                answer = result["answer"]
                sources = result["sources"]

                st.markdown(answer)

                with st.expander("Sources used from dbt project", expanded=False):
                    for src in sources:
                        score = src.get("relevance_score", 0)
                        st.markdown(
                            f"- **`{src['model']}`** ({src['type']}) "
                            f"schema: `{src['schema']}` "
                            f"relevance: `{score}`"
                        )

                st.session_state["messages"].append({
                    "role": "assistant",
                    "content": answer,
                    "sources": sources,
                })

            except Exception as e:
                st.error(f"Error: {e}")
