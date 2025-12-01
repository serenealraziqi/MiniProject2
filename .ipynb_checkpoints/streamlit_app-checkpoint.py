import os, csv, sys, time
import streamlit as st
import pandas as pd
import psycopg2
import bcrypt
import io
import psycopg2.sql as sql
from psycopg2.extras import execute_values
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# Allow big CSV fields safely
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

# -------------------- secrets/env helpers --------------------
def get_secret(key, default=None):
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)

def ensure_sslmode_require(url: str) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    q.setdefault("sslmode", "require")
    return urlunparse(u._replace(query=urlencode(q)))

def get_db_url() -> str | None:
    # Prefer DATABASE_URL from Render env vars
    url = get_secret("DATABASE_URL")
    if url:
        return ensure_sslmode_require(url)

    # Fallback split vars
    u = get_secret("POSTGRES_USERNAME")
    p = get_secret("POSTGRES_PASSWORD")
    h = get_secret("POSTGRES_SERVER")
    d = get_secret("POSTGRES_DATABASE")
    if all([u, p, h, d]):
        return ensure_sslmode_require(f"postgresql://{u}:{p}@{h}/{d}")

    return None

# -------------------- login --------------------
def login_screen():
    st.title("🔒 Secure Login")
    raw_hash = get_secret("HASHED_PASSWORD")
    if not raw_hash:
        st.error("Missing HASHED_PASSWORD in Streamlit secrets or Render env vars.")
        st.stop()

    pw = st.text_input("Password", type="password")
    if st.button("Login", type="primary", use_container_width=True):
        ok = bcrypt.checkpw(pw.encode("utf-8"), raw_hash.encode("utf-8"))
        if ok:
            st.session_state.logged_in = True
            st.rerun()
        else:
            st.error("❌ Incorrect password")

def require_login():
    if not st.session_state.get("logged_in", False):
        login_screen()
        st.stop()

# -------------------- postgres helpers --------------------
def pg_conn():
    url = get_db_url()
    if not url:
        st.error("Missing Postgres connection. Set DATABASE_URL in Render.")
        st.stop()

    # stable connection settings
    conn = psycopg2.connect(
        url,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    conn.autocommit = True
    return conn

def pg_query(sql: str) -> pd.DataFrame:
    conn = pg_conn()
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()

def pg_exec(sql: str):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()

# -------------------- MP2 schema + loader --------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS region (
  regionid SERIAL PRIMARY KEY,
  region TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS country (
  countryid SERIAL PRIMARY KEY,
  country TEXT NOT NULL UNIQUE,
  regionid INTEGER NOT NULL REFERENCES region(regionid)
);
CREATE TABLE IF NOT EXISTS customer (
  customerid SERIAL PRIMARY KEY,
  firstname TEXT NOT NULL,
  lastname TEXT NOT NULL,
  address TEXT,
  city TEXT,
  countryid INTEGER NOT NULL REFERENCES country(countryid)
);
CREATE TABLE IF NOT EXISTS productcategory (
  productcategoryid SERIAL PRIMARY KEY,
  productcategory TEXT NOT NULL UNIQUE,
  productcategorydescription TEXT
);
CREATE TABLE IF NOT EXISTS product (
  productid SERIAL PRIMARY KEY,
  productname TEXT NOT NULL UNIQUE,
  productunitprice NUMERIC NOT NULL,
  productcategoryid INTEGER NOT NULL REFERENCES productcategory(productcategoryid)
);
CREATE TABLE IF NOT EXISTS orderdetail (
  orderid SERIAL PRIMARY KEY,
  customerid INTEGER NOT NULL REFERENCES customer(customerid),
  productid  INTEGER NOT NULL REFERENCES product(productid),
  orderdate  DATE NOT NULL,
  quantityordered INTEGER NOT NULL
);
"""

def load_raw_to_postgres(data_path: str):
    """
    Loads the tab-delimited file into raw_data using COPY (fast + stable).
    """
    delimiter = "\t"

    # Read header safely (utf-8-sig handles BOM if present)
    with open(data_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)

    cols = [c.strip().replace(" ", "_").lower() for c in header]
    if any(not c for c in cols):
        raise ValueError("Bad header: contains empty column name(s).")

    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])  # quote identifiers

    # Use a dedicated connection for load; autocommit OFF is better for COPY
    conn = pg_conn()
    conn.autocommit = False

    status = st.empty()
    prog = st.progress(0)

    try:
        with conn.cursor() as cur:
            status.write("Dropping/creating raw_data…")
            cur.execute("DROP TABLE IF EXISTS raw_data;")
            cur.execute(f"CREATE TABLE raw_data ({col_defs});")
            conn.commit()

            status.write("COPY loading into raw_data…")

            # Re-open file and stream to COPY
            with open(data_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
                copy_sql = f"""
                    COPY raw_data ({",".join([f'"{c}"' for c in cols])})
                    FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', HEADER true, QUOTE '\"', ESCAPE '\"');
                """
                cur.copy_expert(copy_sql, f)

            conn.commit()
            prog.progress(100)
            status.success("✅ Done. Loaded file into `raw_data` using COPY.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def strip_code_fences(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("```"):
        lines = s.splitlines()
        # drop first line like ``` or ```sql
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        # drop last line ```
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    return s

# -------------------- OpenAI -> SQL (Postgres) --------------------
def ai_to_sql(question: str) -> str:
    api_key = get_secret("OPENAI_API_KEY")
    if not api_key:
        st.error("Missing OPENAI_API_KEY in secrets/env.")
        st.stop()

    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        st.error("OpenAI SDK missing. Install: pip install openai")
        st.stop()

    client = OpenAI(api_key=api_key)
    model = get_secret("OPENAI_MODEL", "gpt-4o-mini")

    system = (
        "You write PostgreSQL SELECT queries only. No INSERT/UPDATE/DELETE/DROP.\n"
        "Schema (public): region, country, customer, productcategory, product, orderdetail, raw_data.\n"
        "Use lowercase table/columns. Return ONLY SQL."
    )

    resp = client.chat.completions.create(
        model=model,
        temperature=0.1,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": question.strip()},
        ],
    )
    return resp.choices[0].message.content.strip()

# -------------------- UI --------------------
st.set_page_config(page_title="MP2 Postgres + OpenAI", page_icon="🧠")

def main():
    require_login()

    st.sidebar.button("🚪 Logout", on_click=lambda: st.session_state.update({"logged_in": False}))
    st.sidebar.markdown("---")

    st.sidebar.markdown("### Setup")
    data_path = st.sidebar.text_input("data file path", value="data.csv")

    # Create tables once
    if st.sidebar.button("1) Create tables in Postgres", type="primary", use_container_width=True):
        pg_exec(SCHEMA_SQL)
        st.sidebar.success("Tables created/ensured ✅")

    # Load raw file only when button is clicked
    if st.sidebar.button("2) Load file into raw_data", use_container_width=True):
        if not os.path.exists(data_path):
            st.sidebar.error(f"Can't find {data_path}")
        else:
            load_raw_to_postgres(data_path)
            st.sidebar.success("Raw loaded ✅")

    if st.sidebar.button("List tables", use_container_width=True):
        df = pg_query("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema','pg_catalog')
            ORDER BY table_schema, table_name;
        """)
        st.sidebar.dataframe(df, use_container_width=True)

    st.title("🧠 MP2 — Postgres SQL Assistant")

    st.subheader("Ask a question (AI generates SQL)")
    # make sure the widget state exists
    if "sql_editor" not in st.session_state:
        st.session_state.sql_editor = ""

    st.subheader("Ask a question (AI generates SQL)")
    q = st.text_area("Question", height=80, placeholder="e.g., Show me 10 rows from raw_data")

    if st.button("Generate SQL", type="primary"):
        if q.strip():
            st.session_state.sql_editor = ai_to_sql(q)   # ✅ write into the textbox state
        else:
            st.warning("Type a question first.")

    st.text_area("SQL (edit if needed)", height=160, key="sql_editor")

    if st.button("▶️ Run Query", use_container_width=True):
        user_sql = st.session_state.sql_editor.strip()
        if not user_sql:
            st.warning("No SQL to run.")
        else:
            first = user_sql.split(None, 1)[0].lower()
            if first not in ("select", "with"):
                st.error("Only SELECT/WITH queries allowed here.")
            else:
                try:
                    df = pg_query(user_sql)
                    st.success(f"Returned {len(df)} rows")
                    st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error("SQL error:")
                    st.exception(e)

    st.markdown("---")
    st.subheader("Run custom SQL (SELECT only)")
    custom = st.text_area("Custom SQL", height=140, placeholder="SELECT * FROM raw_data LIMIT 10;")
    if st.button("Run custom SQL"):
        s = custom.strip()
        if not s:
            st.warning("No SQL.")
        else:
            first = s.split(None, 1)[0].lower()
            if first not in ("select", "with"):
                st.error("Only SELECT/WITH queries allowed.")
            else:
                try:
                    df = pg_query(s)
                    st.success(f"Returned {len(df)} rows")
                    st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error("SQL error:")
                    st.exception(e)

if __name__ == "__main__":
    main()
