from fastapi import FastAPI, HTTPException, Query
from .db import get_conn

app = FastAPI(title="SEC XBRL FinWarehouse", version="0.1.0")

@app.get("/company/{ticker}")
def company(ticker: str):
    ticker = ticker.upper()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cik, ticker, name FROM companies WHERE ticker=%s",
                (ticker,),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Ticker not found")
    cik, ticker, name = row
    return {"cik": cik, "ticker": ticker, "name": name}

@app.get("/ratios/{ticker}")
def ratios(ticker: str, limit: int = Query(10, ge=1, le=50)):
    ticker = ticker.upper()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT cik FROM companies WHERE ticker=%s", (ticker,))
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Ticker not found")
            cik = r[0]

            cur.execute(
                """
                SELECT fiscal_year, gross_margin, operating_margin, net_margin,
                       roa, roe, leverage, fcf_margin, asset_turnover
                FROM ratios_annual
                WHERE cik=%s
                ORDER BY fiscal_year DESC
                LIMIT %s
                """,
                (cik, limit),
            )
            rows = cur.fetchall()

    return {
        "ticker": ticker,
        "years": [
            {
                "fiscal_year": fy,
                "gross_margin": gm,
                "operating_margin": om,
                "net_margin": nm,
                "roa": roa,
                "roe": roe,
                "leverage": lev,
                "fcf_margin": fcfm,
                "asset_turnover": at,
            }
            for (fy, gm, om, nm, roa, roe, lev, fcfm, at) in rows
        ],
    }

@app.get("/screener")
def screener(
    min_roe: float | None = None,
    min_fcf_margin: float | None = None,
    min_net_margin: float | None = None,
    year: int | None = None,
    limit: int = Query(25, ge=1, le=200),
):
    filters = []
    params = []

    if year is not None:
        filters.append("r.fiscal_year = %s")
        params.append(year)

    if min_roe is not None:
        filters.append("r.roe >= %s")
        params.append(min_roe)

    if min_fcf_margin is not None:
        filters.append("r.fcf_margin >= %s")
        params.append(min_fcf_margin)

    if min_net_margin is not None:
        filters.append("r.net_margin >= %s")
        params.append(min_net_margin)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    sql = f"""
        SELECT c.ticker, c.name, r.fiscal_year, r.roe, r.fcf_margin, r.net_margin
        FROM ratios_annual r
        JOIN companies c ON c.cik = r.cik
        {where}
        ORDER BY r.fiscal_year DESC, r.roe DESC NULLS LAST
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    return {
        "results": [
            {
                "ticker": t,
                "name": n,
                "fiscal_year": fy,
                "roe": roe,
                "fcf_margin": fcfm,
                "net_margin": nm,
            }
            for (t, n, fy, roe, fcfm, nm) in rows
        ]
    }