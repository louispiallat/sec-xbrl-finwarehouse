import os
from datetime import date
from typing import Any, Dict, List, Tuple, Optional, Set

import psycopg2
from dotenv import load_dotenv

from sec_xbrl_finwarehouse.sec_client import SecClient

# Minimal set of statement tags for V1 (enough for later ratios)
CORE_TAGS = {
    "Revenues",
    "GrossProfit",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "NetCashProvidedByUsedInOperatingActivities",
    "PaymentsToAcquirePropertyPlantAndEquipment",  # CAPEX proxy
}

FilingRow = Tuple[str, str, Optional[str], Optional[date], Optional[date], Optional[int], Optional[str]]
FactRow = Tuple[str, str, str, str, Optional[date], Optional[date], float, Optional[str], Optional[str], Optional[date], Optional[str]]

def _d(s: Optional[str]) -> Optional[date]:
    return date.fromisoformat(s) if s else None

def extract_filings_and_facts(company_json: Dict[str, Any], cik10: str) -> Tuple[List[FilingRow], List[FactRow]]:
    facts = company_json.get("facts", {})
    us_gaap = facts.get("us-gaap", {})

    filings_map: dict[str, FilingRow] = {}
    fact_rows: List[FactRow] = []

    for tag, payload in us_gaap.items():
        if tag not in CORE_TAGS:
            continue

        units = payload.get("units", {})
        for item in units.get("USD", []):
            val = item.get("val")
            if val is None:
                continue

            accn = item.get("accn")  # accession number
            form = item.get("form")
            filed = _d(item.get("filed"))
            period_start = _d(item.get("start"))
            period_end = _d(item.get("end"))
            frame = item.get("frame")
            fy = item.get("fy")
            fp = item.get("fp")

            # 1) Prepare filings row (so FK in facts won't fail)
            if accn:
                # filings table: accession_no, cik, form, filing_date, report_date, fiscal_year, fiscal_period
                # report_date: we use period_end as a reasonable proxy in V1
                filings_map[accn] = (accn, cik10, form, filed, period_end, int(fy) if fy is not None else None, fp)

            # 2) Prepare fact row
            fact_rows.append(
                (
                    cik10,
                    "us-gaap",
                    tag,
                    "USD",
                    period_start,
                    period_end,
                    float(val),
                    accn,
                    form,
                    filed,
                    frame,
                )
            )

    return list(filings_map.values()), fact_rows

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Missing DATABASE_URL in .env")

    client = SecClient()

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT cik, ticker FROM companies ORDER BY ticker;")
            companies = cur.fetchall()

        total_facts_attempted = 0
        total_filings_attempted = 0

        for cik, ticker in companies:
            print(f"→ Fetching {ticker} (CIK {cik})")
            data = client.get_company_facts(cik)

            filing_rows, fact_rows = extract_filings_and_facts(data, cik)

            if not fact_rows:
                print(f"  ⚠️  No CORE_TAGS facts found for {ticker}")
                continue

            # Insert filings first (to satisfy FK constraint)
            if filing_rows:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO filings (
                          accession_no, cik, form, filing_date, report_date, fiscal_year, fiscal_period
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (accession_no) DO NOTHING
                        """,
                        filing_rows,
                    )
                total_filings_attempted += len(filing_rows)

            # Then insert facts
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO facts (
                      cik, taxonomy, tag, unit, period_start, period_end, value,
                      filing_accession_no, form, filed, frame
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    fact_rows,
                )

            conn.commit()
            total_facts_attempted += len(fact_rows)

            print(f"  ✅ Filings upsert attempted: {len(filing_rows)}")
            print(f"  ✅ Facts insert attempted: {len(fact_rows)}")

        print(f"\n✅ Done. Filings attempted: {total_filings_attempted} | Facts attempted: {total_facts_attempted}")

if __name__ == "__main__":
    main()