import os
import time
from typing import Any, Dict, Optional

import requests

SEC_BASE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"


class SecClient:
    def __init__(self, user_agent: Optional[str] = None, timeout: int = 30):
        self.user_agent = user_agent or os.getenv("SEC_USER_AGENT")
        if not self.user_agent:
            raise ValueError("Missing SEC_USER_AGENT env var (use: 'Name email@domain.com').")

        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
                "Host": "data.sec.gov",
            }
        )

    def get_company_facts(self, cik: str, retries: int = 3, backoff: float = 1.6) -> Dict[str, Any]:
        cik10 = cik.zfill(10)
        url = SEC_BASE.format(cik=cik10)

        last_err: Optional[Exception] = None
        for attempt in range(retries):
            try:
                r = self.session.get(url, timeout=self.timeout)
                if r.status_code == 200:
                    time.sleep(0.2)  # gentle pacing
                    return r.json()

                # retry on rate limiting / transient errors
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(backoff ** (attempt + 1))
                    continue

                r.raise_for_status()

            except Exception as e:
                last_err = e
                time.sleep(backoff ** (attempt + 1))

        raise RuntimeError(f"Failed to fetch SEC company facts for CIK={cik}: {last_err}")
