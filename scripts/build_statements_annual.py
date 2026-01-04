import os
from dotenv import load_dotenv
import psycopg2

TAGS = {
    "revenues": "Revenues",
    "gross_profit": "GrossProfit",
    "operating_income": "OperatingIncomeLoss",
    "net_income": "NetIncomeLoss",
    "total_assets": "Assets",
    "total_liabilities": "Liabilities",
    "total_equity": "StockholdersEquity",
    "operating_cash_flow": "NetCashProvidedByUsedInOperatingActivities",
    "capex": "PaymentsToAcquirePropertyPlantAndEquipment",
}

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Missing DATABASE_URL in .env")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            # For each filing (accession), we have fiscal_year/fiscal_period in filings.
            # We want annual rows (fp = 'FY') primarily.
            cur.execute("""
                WITH ranked AS (
                  SELECT
                    f.cik,
                    fl.fiscal_year,
                    f.tag,
                    f.value,
                    f.filed,
                    f.form,
                    ROW_NUMBER() OVER (
                      PARTITION BY f.cik, fl.fiscal_year, f.tag
                      ORDER BY
                        (CASE WHEN f.form = '10-K' THEN 0 ELSE 1 END),
                        f.filed DESC NULLS LAST
                    ) AS rn
                  FROM facts f
                  JOIN filings fl
                    ON fl.accession_no = f.filing_accession_no
                  WHERE fl.fiscal_year IS NOT NULL
                    AND fl.fiscal_period IN ('FY')   -- keep annual
                    AND f.unit = 'USD'
                )
                SELECT cik, fiscal_year, tag, value
                FROM ranked
                WHERE rn = 1;
            """)
            rows = cur.fetchall()

        # reshape into dict keyed by (cik, fiscal_year)
        by_year = {}
        for cik, fy, tag, value in rows:
            key = (cik, fy)
            by_year.setdefault(key, {})[tag] = value

        upserts = []
        for (cik, fy), tags in by_year.items():
            revenues = tags.get(TAGS["revenues"])
            gross_profit = tags.get(TAGS["gross_profit"])
            operating_income = tags.get(TAGS["operating_income"])
            net_income = tags.get(TAGS["net_income"])
            total_assets = tags.get(TAGS["total_assets"])
            total_liabilities = tags.get(TAGS["total_liabilities"])
            total_equity = tags.get(TAGS["total_equity"])
            ocf = tags.get(TAGS["operating_cash_flow"])
            capex = tags.get(TAGS["capex"])

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

        print(f"✅ Upserted statements_annual rows: {len(upserts)}")

if __name__ == "__main__":
    main()