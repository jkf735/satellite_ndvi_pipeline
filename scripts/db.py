import os
import psycopg2
from dotenv import load_dotenv

def get_db_connection():
    """
    Returns a new PostgreSQL connection.
    Raises error if required env vars missing.
    """
    # load environment variables once
    load_dotenv()
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    if not all([DB_NAME, DB_USER, DB_PASSWORD]):
        raise ValueError("Missing required DB environment variables.")
    
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )