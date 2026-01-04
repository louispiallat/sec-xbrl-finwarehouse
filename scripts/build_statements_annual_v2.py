import os
from dotenv import load_dotenv
import psycopg2

REVENUE_CANDIDATES = (
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "TotalRevenues",
)

TAG_MAP = {
    "gross_profit": ("GrossProfit",),
    "operating_income": ("OperatingIncomeLoss",),
    "net_income": ("NetIncomeLoss",),
    "total_assets": ("Assets",),
    "total_liabilities": ("Liabilities",),
    "total_equity": ("StockholdersEquity",),
    "operating_cash_flow": ("NetCashProvidedByUsedInOperatingActivities",),
    "capex": ("PaymentsToAcquirePropertyPlantAndEquipment",),
}

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Missing DATABASE_URL in .env")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            # Pull ONE best value per (cik, fiscal_year, tag) with priority to 10-K
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
                    AND fl.fiscal_period = 'FY'
                    AND f.unit = 'USD'
                )
                SELECT cik, fiscal_year, tag, value, form
                FROM ranked
                WHERE rn = 1;
            """)
            rows = cur.fetchall()

        # Reshape values
        by_year = {}
        for cik, fy, tag, value, form in rows:
            by_year.setdefault((cik, fy), {})[tag] = (value, form)

        upserts = []
        for (cik, fy), tags in by_year.items():
            # Revenues: pick first candidate available
            revenues = None
            rev_form = None
            for cand in REVENUE_CANDIDATES:
                if cand in tags:
                    revenues, rev_form = tags[cand]
                    break

            def pick_one(field: str):
                for t in TAG_MAP[field]:
                    if t in tags:
                        return tags[t][0]
                return None

            gross_profit = pick_one("gross_profit")
            operating_income = pick_one("operating_income")
            net_income = pick_one("net_income")
            total_assets = pick_one("total_assets")
            total_liabilities = pick_one("total_liabilities")
            total_equity = pick_one("total_equity")
            ocf = pick_one("operating_cash_flow")
            capex = pick_one("capex")

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

        print(f"✅ V2 upserted statements_annual rows: {len(upserts)}")

if __name__ == "__main__":
    main()