import os
import requests
import psycopg2
from dotenv import load_dotenv

TICKER_CIK_URL = "https://www.sec.gov/files/company_tickers.json"

def get_ticker_cik_map(user_agent: str) -> dict:
    r = requests.get(
        TICKER_CIK_URL,
        headers={"User-Agent": user_agent, "Accept": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()  # dict of numeric keys -> {cik_str, ticker, title}
    out = {}
    for _, row in data.items():
        out[row["ticker"].upper()] = {
            "cik": str(row["cik_str"]).zfill(10),
            "name": row["title"],
        }
    return out

def main():
    load_dotenv()
    user_agent = os.getenv("SEC_USER_AGENT")
    db_url = os.getenv("DATABASE_URL")
    tickers = os.getenv("TICKERS", "")

    if not user_agent:
        raise ValueError("Missing SEC_USER_AGENT in .env")
    if not db_url:
        raise ValueError("Missing DATABASE_URL in .env")
    if not tickers.strip():
        raise ValueError("Missing TICKERS in .env (comma-separated)")

    tickers_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    mapping = get_ticker_cik_map(user_agent)

    rows = []
    for t in tickers_list:
        if t not in mapping:
            print(f"⚠️  Ticker not found in SEC mapping: {t}")
            continue
        rows.append((mapping[t]["cik"], t, mapping[t]["name"]))

    if not rows:
        raise RuntimeError("No valid tickers found to insert.")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO companies (cik, ticker, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (cik) DO UPDATE
                  SET ticker = EXCLUDED.ticker,
                      name = EXCLUDED.name
                """,
                rows,
            )
        conn.commit()

    print(f"✅ Inserted/updated {len(rows)} companies.")

if __name__ == "__main__":
    main()
