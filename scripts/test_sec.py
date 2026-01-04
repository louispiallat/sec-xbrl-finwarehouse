import os
from dotenv import load_dotenv

from sec_xbrl_finwarehouse.sec_client import SecClient

if __name__ == "__main__":
    load_dotenv()
    client = SecClient()
    data = client.get_company_facts("0000320193")  # Apple
    print(data["entityName"], data["cik"])
    print("taxonomies:", list(data["facts"].keys())[:5])