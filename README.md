# SEC XBRL FinWarehouse

Mini financial data warehouse built from SEC EDGAR XBRL “company facts”, stored in Postgres, served via a FastAPI API (ratios + screener).

## What it does (V1)
- Fetch SEC XBRL company facts for a small list of tickers (10)
- Normalize a subset of annual statement items into SQL tables
- Compute a basic ratio set (profitability, leverage, cash-flow)
- Expose endpoints:
  - `GET /company/{ticker}`
  - `GET /ratios/{ticker}`
  - `GET /screener?...`

## Stack
Python · Postgres · FastAPI

## Data source
SEC EDGAR XBRL Company Facts (public). Please use a real `SEC_USER_AGENT` (name + email).

## Quickstart (local)
1. Set env vars (see `.env.example`)
2. Create tables (`db/schema.sql`)
3. Run ingestion (`python scripts/ingest.py`)
4. Run API (`uvicorn api.main:app --reload`)

## Roadmap
- Dedup + data quality checks
- Cache + retries
- Small Streamlit dashboard
