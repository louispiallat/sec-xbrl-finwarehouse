import os
from dotenv import load_dotenv
import psycopg2

REVENUE_CANDIDATES = (
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
    "Revenues",
    "TotalRevenues",
)

FLOW_TAGS = (
    # revenue candidates + P&L + cashflow
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
    "Revenues",
    "TotalRevenues",
    "GrossProfit",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "NetCashProvidedByUsedInOperatingActivities",
    "PaymentsToAcquirePropertyPlantAndEquipment",
)

STOCK_TAGS = (
    "Assets",
    "Liabilities",
    "StockholdersEquity",
)

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Missing DATABASE_URL in .env")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            # 1) FLOW items: keep annual-like periods (~1 year) and 10-K
            cur.execute(
                """
                WITH base AS (
                  SELECT
                    cik,
                    EXTRACT(YEAR FROM period_end)::int AS fiscal_year,
                    tag,
                    value,
                    filed,
                    form,
                    (period_end - period_start) AS days
                  FROM facts
                  WHERE taxonomy='us-gaap'
                    AND unit='USD'
                    AND tag = ANY(%s)
                    AND form IN ('10-K', '20-F')
                    AND period_start IS NOT NULL
                    AND period_end IS NOT NULL
                    AND (period_end - period_start) BETWEEN 330 AND 380
                ),
                ranked AS (
                  SELECT *,
                         ROW_NUMBER() OVER (
                           PARTITION BY cik, fiscal_year, tag
                           ORDER BY filed DESC NULLS LAST
                         ) AS rn
                  FROM base
                )
                SELECT cik, fiscal_year, tag, value
                FROM ranked
                WHERE rn = 1;
                """,
                (list(FLOW_TAGS),),
            )
            flow_rows = cur.fetchall()

            # 2) STOCK items: point-in-time at FY end, 10-K
            cur.execute(
                """
                WITH base AS (
                  SELECT
                    cik,
                    EXTRACT(YEAR FROM period_end)::int AS fiscal_year,
                    tag,
                    value,
                    filed,
                    form
                  FROM facts
                  WHERE taxonomy='us-gaap'
                    AND unit='USD'
                    AND tag = ANY(%s)
                    AND form IN ('10-K', '20-F')
                    AND period_end IS NOT NULL
                    AND period_start IS NULL
                ),
                ranked AS (
                  SELECT *,
                         ROW_NUMBER() OVER (
                           PARTITION BY cik, fiscal_year, tag
                           ORDER BY filed DESC NULLS LAST
                         ) AS rn
                  FROM base
                )
                SELECT cik, fiscal_year, tag, value
                FROM ranked
                WHERE rn = 1;
                """,
                (list(STOCK_TAGS),),
            )
            stock_rows = cur.fetchall()

    # Merge rows into dict[(cik, fy)][tag]=value
    by_year = {}
    for cik, fy, tag, val in flow_rows + stock_rows:
        by_year.setdefault((cik, fy), {})[tag] = val

    upserts = []
    for (cik, fy), t in by_year.items():
        # Revenues: first available candidate
        revenues = None
        for cand in REVENUE_CANDIDATES:
            if cand in t:
                revenues = t[cand]
                break

        gross_profit = t.get("GrossProfit")
        operating_income = t.get("OperatingIncomeLoss")
        net_income = t.get("NetIncomeLoss")

        total_assets = t.get("Assets")
        total_liabilities = t.get("Liabilities")
        total_equity = t.get("StockholdersEquity")

        ocf = t.get("NetCashProvidedByUsedInOperatingActivities")
        capex_raw = t.get("PaymentsToAcquirePropertyPlantAndEquipment")

        # Normalize CAPEX to positive outflow if SEC gives negative values
        capex = None
        if capex_raw is not None:
            capex = -capex_raw if capex_raw < 0 else capex_raw

        fcf = None
        if ocf is not None and capex is not None:
            fcf = ocf - capex

        upserts.append(
            (
                cik, fy,
                revenues, gross_profit, operating_income, net_income,
                total_assets, total_liabilities, total_equity,
                ocf, capex, fcf
            )
        )

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO statements_annual (
                  cik, fiscal_year,
                  revenues, gross_profit, operating_income, net_income,
                  total_assets, total_liabilities, total_equity,
                  operating_cash_flow, capex, free_cash_flow
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (cik, fiscal_year) DO UPDATE SET
                  revenues = EXCLUDED.revenues,
                  gross_profit = EXCLUDED.gross_profit,
                  operating_income = EXCLUDED.operating_income,
                  net_income = EXCLUDED.net_income,
                  total_assets = EXCLUDED.total_assets,
                  total_liabilities = EXCLUDED.total_liabilities,
                  total_equity = EXCLUDED.total_equity,
                  operating_cash_flow = EXCLUDED.operating_cash_flow,
                  capex = EXCLUDED.capex,
                  free_cash_flow = EXCLUDED.free_cash_flow,
                  updated_at = now()
            """, upserts)
        conn.commit()

    print(f"✅ V3 upserted statements_annual rows: {len(upserts)}")

if __name__ == "__main__":
    main()