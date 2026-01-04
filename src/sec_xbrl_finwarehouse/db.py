import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Missing DATABASE_URL in .env")
    return psycopg2.connect(db_url)