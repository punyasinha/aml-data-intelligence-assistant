# AML Data Intelligence Assistant

A production-grade Anti-Money Laundering (AML) data platform with an AI-powered assistant that answers questions about the data models, business logic, lineage, and AML rules — using real dbt project metadata.

**[Live Demo →][(https://aml-data-intelligence-assistant.streamlit.app)](https://aml-data-intelligence-assistant-exessgbm6wcdbkprmmmhcb.streamlit.app/)**

---

## Architecture

```
Raw Data (CSV seeds)
        │
        ▼
  Snowflake (AML_DB)
        │
        ▼
   dbt Pipeline
   ├── Staging       stg_customers, stg_transactions, stg_aml_alerts
   ├── Intermediate  int_customer_risk_profile, int_flagged_transactions
   └── Marts         dim_customer, dim_date, fct_transactions, fct_aml_alerts
        │
        ▼
  dbt docs generate → manifest.json + catalog.json
        │
        ▼
  ChromaDB (vector store)   ←   OpenAI text-embedding-3-small
        │
        ▼
  RAG Query Engine   ←   GPT-4o-mini
        │
        ▼
  Streamlit App (Streamlit Community Cloud)
```

| Layer | Technology |
|---|---|
| Data warehouse | Snowflake |
| Transformation | dbt (Kimball dimensional model) |
| Vector store | ChromaDB |
| Embeddings | OpenAI text-embedding-3-small |
| LLM | GPT-4o-mini |
| App | Streamlit |
| Auth | RSA key-pair (no passwords stored) |

---

## Data Model

The platform models AML transaction monitoring across a Kimball dimensional schema:

```
stg_customers ──────────────────────────────────┐
stg_transactions ──► int_flagged_transactions ──► fct_transactions
stg_aml_alerts ─────────────────────────────────┘
                                                  dim_customer
                                                  dim_date
                                                  fct_aml_alerts
```

**AML rules implemented (AUSTRAC-aligned):**
| Rule | Logic |
|---|---|
| `flag_large_cash` | Cash transaction ≥ AUD 10,000 |
| `flag_structuring` | Multiple transactions totalling ≥ AUD 10,000 within 24h |
| `flag_high_risk_country` | Counterparty in FATF grey/black list country |
| `flag_pep_customer` | Customer is a Politically Exposed Person |
| `flag_sanctioned_customer` | Customer matches sanctions list |

---

## RAG Assistant

The assistant embeds dbt model metadata (descriptions, column definitions, lineage, tags, tests) into ChromaDB using OpenAI embeddings. At query time it retrieves the most relevant model context and passes it to GPT-4o-mini to answer questions grounded in the actual project.

**Example questions it can answer:**
- "How is the composite risk score calculated?"
- "Which models feed into fct_aml_alerts?"
- "What is flag_structuring and what threshold does it use?"
- "What dbt tests are applied to fct_transactions?"
- "How are FIU escalations tracked?"

---

## Project Structure

```
data-intelligence-assistant/
├── dbt_project/
│   ├── models/
│   │   ├── staging/          # Raw source cleaning
│   │   ├── intermediate/     # AML rule logic + risk scoring
│   │   └── marts/            # Kimball dims and facts
│   ├── seeds/                # Reference data (AML rules, high-risk countries)
│   ├── tests/                # Custom data quality tests
│   └── target/
│       ├── manifest.json     # Committed — used by RAG assistant
│       └── catalog.json      # Committed — enriches embeddings with row counts
├── rag_assistant/
│   ├── app.py                # Streamlit app
│   ├── ingest.py             # Parse manifest → embed → ChromaDB
│   ├── query.py              # Retrieve + LLM synthesis
│   └── requirements.txt
├── scripts/
│   ├── generate_and_load.py  # Seed raw data into Snowflake
│   └── requirements.txt
├── docs/
│   └── snowflake_dbt_connection.md
└── snowflake_setup.sql       # All Snowflake DDL and permission commands
```

---

## Local Setup

### 1. Prerequisites

- Python 3.12+
- Snowflake account
- OpenAI API key

### 2. Snowflake setup

Run `snowflake_setup.sql` in a Snowflake worksheet. This creates:
- `AML_WH` warehouse (X-Small, auto-suspend 60s)
- `AML_DB` database with `RAW`, `REFERENCE`, `STAGING`, `MARTS` schemas
- `TRANSFORMER` role with least-privilege permissions
- `SVC_DBT` service account (TYPE=SERVICE, RSA key-pair auth only)

### 3. Environment variables

This project uses no `.env` files. Store secrets in your shell or macOS Keychain:

```bash
# ~/.zshrc
export SNOWFLAKE_ACCOUNT="your-account-identifier"
export SNOWFLAKE_PRIVATE_KEY_PATH="$HOME/.snowflake/rsa_key.p8"
export OPENAI_API_KEY=$(security find-generic-password -s "openai-api-key" -w)
```

See [docs/snowflake_dbt_connection.md](docs/snowflake_dbt_connection.md) for RSA key-pair setup.

### 4. Install dependencies

```bash
# dbt + data scripts
python -m venv .venv && source .venv/bin/activate
pip install -r scripts/requirements.txt

# RAG assistant
pip install -r rag_assistant/requirements.txt
```

### 5. Run the dbt pipeline

```bash
cd dbt_project
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate
cd ..
```

### 6. Run the app locally

```bash
cd rag_assistant
streamlit run app.py
```

The app auto-ingests `manifest.json` into ChromaDB on first run (~5–10 seconds).

---

## Deployment

The app is deployed on **Streamlit Community Cloud**.

1. Push `manifest.json` and `catalog.json` to GitHub (both are committed — see `.gitignore`)
2. Connect the repo to Streamlit Community Cloud
3. Set `OPENAI_API_KEY` in the Streamlit secrets dashboard
4. ChromaDB is rebuilt from `manifest.json` on every cold start

To update the assistant after dbt model changes:
```bash
cd dbt_project && dbt docs generate
git add target/manifest.json target/catalog.json
git commit -m "chore: refresh dbt manifest"
git push
```

Streamlit Cloud redeploys automatically on push.

---

## Security

- No passwords stored anywhere in the project
- Snowflake uses RSA key-pair authentication (service account `SVC_DBT`)
- Private key lives in `~/.snowflake/rsa_key.p8` (local only, never committed)
- API keys managed via macOS Keychain locally, Streamlit secrets in production
- `profiles.yml` and `.env` files are gitignored
