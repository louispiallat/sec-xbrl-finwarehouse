import os
from dotenv import load_dotenv
import psycopg2

def safe_div(a, b):
    if a is None or b in (None, 0):
        return None
    return a / b

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Missing DATABASE_URL in .env")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  cik, fiscal_year,
                  revenues, gross_profit, operating_income, net_income,
                  total_assets, total_equity,
                  free_cash_flow
                FROM statements_annual
            """)
            rows = cur.fetchall()

        upserts = []
        for cik, fy, rev, gp, op, ni, assets, equity, fcf in rows:
            gross_margin = safe_div(gp, rev)
            operating_margin = safe_div(op, rev)
            net_margin = safe_div(ni, rev)

            roa = safe_div(ni, assets)
            roe = safe_div(ni, equity)
            leverage = safe_div(assets, equity)

            fcf_margin = safe_div(fcf, rev)
            asset_turnover = safe_div(rev, assets)

            upserts.append(
                (cik, fy, gross_margin, operating_margin, net_margin, roa, roe, leverage, fcf_margin, asset_turnover)
            )

        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO ratios_annual (
                      cik, fiscal_year,
                      gross_margin, operating_margin, net_margin,
                      roa, roe, leverage,
                      fcf_margin, asset_turnover
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (cik, fiscal_year) DO UPDATE SET
                      gross_margin=EXCLUDED.gross_margin,
                      operating_margin=EXCLUDED.operating_margin,
                      net_margin=EXCLUDED.net_margin,
                      roa=EXCLUDED.roa,
                      roe=EXCLUDED.roe,
                      leverage=EXCLUDED.leverage,
                      fcf_margin=EXCLUDED.fcf_margin,
                      asset_turnover=EXCLUDED.asset_turnover
                """, upserts)
            conn.commit()

    print(f"✅ Upserted ratios_annual rows: {len(upserts)}")

if __name__ == "__main__":
    main()