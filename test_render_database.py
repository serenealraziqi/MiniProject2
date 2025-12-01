import psycopg2
from utils import get_db_url

def test_postgress_connection(DATABASE_URL):
    try:
        conn = psycopg2.connect(DATABASE_URL)

        cur = conn.cursor()

        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        print("Connection successful!")
        print(f"PostgresSQL version: {db_version}")

        cur.close()
        conn.close()
        print("Connection closed")
        return True
    except Exception as e:
        print("Connection failed")
        print(e)
        return False

DATABASE_URL = get_db_url()
test_postgress_connection(DATABASE_URL)
