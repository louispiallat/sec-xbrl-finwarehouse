-- db/schema.sql
-- Minimal V1 schema for SEC XBRL company facts -> warehouse

CREATE TABLE IF NOT EXISTS companies (
  cik TEXT PRIMARY KEY,
  ticker TEXT UNIQUE NOT NULL,
  name TEXT,
  sic TEXT,
  sector TEXT,
  industry TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS filings (
  accession_no TEXT PRIMARY KEY,
  cik TEXT NOT NULL REFERENCES companies(cik),
  form TEXT,
  filing_date DATE,
  report_date DATE,
  fiscal_year INT,
  fiscal_period TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Raw facts (narrow table)
CREATE TABLE IF NOT EXISTS facts (
  id BIGSERIAL PRIMARY KEY,
  cik TEXT NOT NULL REFERENCES companies(cik),
  taxonomy TEXT,               -- e.g., "us-gaap"
  tag TEXT NOT NULL,           -- e.g., "Revenues"
  unit TEXT,                   -- e.g., "USD"
  period_start DATE,
  period_end DATE,
  value NUMERIC,
  filing_accession_no TEXT REFERENCES filings(accession_no),
  form TEXT,
  filed DATE,
  frame TEXT,                  -- sometimes present
  UNIQUE (cik, taxonomy, tag, unit, period_start, period_end, value, filed)
);

-- Annual normalized statement (wide-ish table for quick ratios)
CREATE TABLE IF NOT EXISTS statements_annual (
  cik TEXT NOT NULL REFERENCES companies(cik),
  fiscal_year INT NOT NULL,
  currency TEXT DEFAULT 'USD',

  revenues NUMERIC,
  gross_profit NUMERIC,
  operating_income NUMERIC,
  net_income NUMERIC,

  total_assets NUMERIC,
  total_liabilities NUMERIC,
  total_equity NUMERIC,

  operating_cash_flow NUMERIC,
  capex NUMERIC,
  free_cash_flow NUMERIC,

  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (cik, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_facts_cik_tag_end ON facts (cik, tag, period_end);
CREATE INDEX IF NOT EXISTS idx_filings_cik_date ON filings (cik, filing_date);

CREATE TABLE IF NOT EXISTS ratios_annual (
  cik TEXT NOT NULL REFERENCES companies(cik),
  fiscal_year INT NOT NULL,
  gross_margin DOUBLE PRECISION,
  operating_margin DOUBLE PRECISION,
  net_margin DOUBLE PRECISION,
  roa DOUBLE PRECISION,
  roe DOUBLE PRECISION,
  leverage DOUBLE PRECISION,
  fcf_margin DOUBLE PRECISION,
  asset_turnover DOUBLE PRECISION,
  PRIMARY KEY (cik, fiscal_year)
);