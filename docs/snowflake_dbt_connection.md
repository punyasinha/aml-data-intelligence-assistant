# How dbt Connects to Snowflake

## Overview

dbt connects to Snowflake using the Snowflake Python connector under the hood.
The connection is configured in `profiles.yml` and authenticated via RSA key-pair —
no password exists for the service account.

## Authentication Flow

```
Developer terminal
└── ~/.zshrc exports environment variables:
    ├── SNOWFLAKE_ACCOUNT    — account identifier (e.g. mhexpqg-xo01544)
    └── SNOWFLAKE_PRIVATE_KEY_PATH — path to private key (~/.snowflake/rsa_key.p8)

dbt reads profiles.yml
└── Resolves env vars via {{ env_var('...') }}
└── Connects to Snowflake as user SVC_DBT
    └── Authenticates using private key (~/.snowflake/rsa_key.p8)
        └── Snowflake verifies against registered public key (stored in Snowflake)
            └── Connection established — dbt runs SQL as role TRANSFORMER
```

## Key Files

| File | Purpose |
|------|---------|
| `dbt_project/profiles.yml` | dbt connection config — references env vars, never hardcodes credentials |
| `~/.snowflake/rsa_key.p8` | Private key — stays on local machine, never committed to git |
| `~/.snowflake/rsa_key.pub` | Public key — registered in Snowflake against SVC_DBT |
| `~/.zshrc` | Exports env vars at shell startup — no credentials written to project files |

## profiles.yml (dev target)

```yaml
aml_intelligence:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: SVC_DBT
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: TRANSFORMER
      database: AML_DB
      warehouse: AML_WH
      schema: DEV
      threads: 4
```

## Snowflake Side

- **User:** `SVC_DBT` — TYPE=SERVICE (no MFA, no password)
- **Role:** `TRANSFORMER` — least privilege, only what dbt needs
- **Warehouse:** `AML_WH` — X-Small, auto-suspends after 60s
- **Database:** `AML_DB` — contains RAW, REFERENCE, STAGING, MARTS schemas

## Why Key-Pair Auth?

| Approach | Used by | Why |
|----------|---------|-----|
| Password | Never for service accounts | Can be leaked, requires rotation |
| MFA | Human users only | Not suitable for automated tools |
| Key-pair | Service accounts, CI/CD | No secret transmitted, private key never leaves the machine |

The private key signs a token locally. Snowflake verifies the signature using the
registered public key. The private key itself is never sent over the network.

## Running dbt

```bash
# Activate virtual environment
source .venv/bin/activate

# Run all models
dbt run

# Run tests
dbt test

# Generate docs (produces manifest.json used by the RAG assistant)
dbt docs generate
```

## CI/CD (prod target)

In GitHub Actions, the private key is stored as a GitHub Actions secret and
injected at runtime — the same pattern, but no human is involved:

```yaml
- name: Run dbt
  env:
    SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
    SNOWFLAKE_PRIVATE_KEY_PATH: /tmp/rsa_key.p8
  run: |
    echo "${{ secrets.SNOWFLAKE_PRIVATE_KEY }}" > /tmp/rsa_key.p8
    dbt run --target prod
```
