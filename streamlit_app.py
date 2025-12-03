import os, csv, sys, re
import streamlit as st
import pandas as pd
import psycopg2
import bcrypt
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from psycopg2.pool import SimpleConnectionPool

st.set_page_config(page_title="🤖 AI-Powered SQL Query Assistant", page_icon="🤖", layout="wide")

max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

def get_secret(key: str, default=None):
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)

def ensure_sslmode_require(url: str) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    q.setdefault("sslmode", "require")
    return urlunparse(u._replace(query=urlencode(q)))

@st.cache_resource
def get_db_url() -> str | None:
    # Prefer DATABASE_URL (Render convention)
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

def login_screen():
    st.title("🔒 Login")
    st.markdown("---")
    st.write("Enter your password.🤫")

    raw_hash = get_secret("HASHED_PASSWORD")
    if not raw_hash:
        st.error("Missing HASHED_PASSWORD in Streamlit secrets or Render env vars.")
        st.stop()

    pw = st.text_input("Password", type="password", key="login_password")
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        login_btn = st.button("Login", type="primary", use_container_width=True)

    if login_btn:
        if pw:
            try:
                ok = bcrypt.checkpw(pw.encode("utf-8"), raw_hash.encode("utf-8"))
                if ok:
                    st.session_state.logged_in = True
                    st.success("✅ Authentication successful. Redirecting...")
                    st.rerun()
                else:
                    st.error("😱 Incorrect Password")
            except Exception as e:
                st.error(f"Authentication error: {e}")
        else:
            st.warning("⚠️ Please enter a password")



def require_login():
    if not st.session_state.get("logged_in", False):
        login_screen()
        st.stop()

@st.cache_resource
def get_pg_pool() -> SimpleConnectionPool:
    url = get_db_url()
    if not url:
        # Don't st.stop() inside cache function; raise instead
        raise RuntimeError("Missing Postgres connection. Set DATABASE_URL (recommended) or split POSTGRES_* vars.")

    return SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        dsn=url,
    )

def pg_query(q: str) -> pd.DataFrame:
    pool = get_pg_pool()
    conn = pool.getconn()
    try:
        # a small guard; you also enforce SELECT/WITH later
        return pd.read_sql_query(q, conn)
    finally:
        pool.putconn(conn)

def pg_exec(stmt: str):
    pool = get_pg_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(stmt)
    finally:
        pool.putconn(conn)

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
    Loads a tab-delimited file into raw_data using COPY (fast + stable).
    """
    url = get_db_url()
    if not url:
        st.error("Missing Postgres connection. Set DATABASE_URL in Render.")
        st.stop()

    delimiter = "\t"

    # Read header safely (utf-8-sig handles BOM if present)
    with open(data_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)

    cols = [c.strip().replace(" ", "_").lower() for c in header]
    if any(not c for c in cols):
        raise ValueError("Bad header: contains empty column name(s).")

    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])  # quote identifiers

    # Use a dedicated connection for load (COPY is best with a single connection)
    conn = psycopg2.connect(
        url,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
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

# -------------------- OpenAI -> SQL --------------------
DATABASE_SCHEMA_HINT = """
Schema (public):
- region(regionid, region)
- country(countryid, country, regionid)
- customer(customerid, firstname, lastname, address, city, countryid)
- productcategory(productcategoryid, productcategory, productcategorydescription)
- product(productid, productname, productunitprice, productcategoryid)
- orderdetail(orderid, customerid, productid, orderdate, quantityordered)
- raw_data(<loaded from file>)
"""

def extract_sql_from_response(response_text: str) -> str | None:
    if not response_text:
        return None

    # fenced SQL ```sql ... ```
    sql_pattern = r"```sql\s*(.*?)\s*```"
    matches = re.findall(sql_pattern, response_text, re.DOTALL | re.IGNORECASE)
    if matches:
        return matches[0].strip()

    # any code block ``` ... ```
    code_pattern = r"```\s*(.*?)\s*```"
    matches = re.findall(code_pattern, response_text, re.DOTALL)
    if matches:
        return matches[0].strip()

    # fallback: scan for SQL keyword
    sql_start = re.search(r"\b(SELECT|WITH)\b", response_text, re.IGNORECASE)
    if sql_start:
        return response_text[sql_start.start():].strip()

    return response_text.strip()

@st.cache_resource
def get_openai_client():
    api_key = get_secret("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY in secrets/env.")
    from openai import OpenAI  # type: ignore
    return OpenAI(api_key=api_key)

def generate_sql_with_gpt(user_question: str) -> tuple[str | None, str | None]:
    client = get_openai_client()
    model = get_secret("OPENAI_MODEL", "gpt-4o-mini")

    system = (
        "You are a PostgreSQL expert. Produce a single SQL query.\n"
        "Rules:\n"
        "- Only SELECT/WITH queries. Never write INSERT/UPDATE/DELETE/DROP/CREATE.\n"
        "- Use proper JOINs.\n"
        "- Prefer LIMIT 100 when user doesn't specify.\n\n"
        f"{DATABASE_SCHEMA_HINT}"
    )

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_question.strip()},
            ],
            temperature=0.1,
            max_tokens=900,
        )
        raw = resp.choices[0].message.content or ""
        sql_q = extract_sql_from_response(raw)
        return sql_q, raw
    except Exception as e:
        st.error(f"Error calling OpenAI API: {e}")
        return None, None

# -------------------- UI --------------------
def main():
    require_login()

    # Sidebar — examples + setup (like your first app vibe)
    st.sidebar.title("💡 Example Questions")
    st.sidebar.markdown(
        "- How many customers do we have by country?\n"
        "- What is total sales by quarter?\n"
        "- Which products are most ordered?\n"
        "- Show me 10 rows from raw_data"
    )

    st.sidebar.info(
        "**How it works:**\n"
        "1. Enter your question in plain English\n"
        "2. AI generates a SQL query\n"
        "3. Review and edit if needed\n"
        "4. Click **Run Query** to execute"
    )

    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 Logout", use_container_width=True):
        st.session_state.logged_in = False
        st.success("You have been logged out.")
        st.rerun()

    # Main header
    st.title("🤖 AI-Powered SQL Query Assistant")
    st.markdown("Ask questions in natural language, and I will generate SQL queries for you to review and run!")
    st.markdown("---")

    # Session state (like your first app)
    if "query_history" not in st.session_state:
        st.session_state.query_history = []
    if "generated_sql" not in st.session_state:
        st.session_state.generated_sql = None
    if "raw_response" not in st.session_state:
        st.session_state.raw_response = None
    if "current_question" not in st.session_state:
        st.session_state.current_question = None

    user_question = st.text_area(
        "💬 What would you like to know?",
        height=100,
        placeholder="e.g., What is total revenue by product category?"
    )

    col1, col2, _ = st.columns([1, 1, 4])
    with col1:
        generate_button = st.button("Run SQL", type="primary", use_container_width=True)
    with col2:
        if st.button("Clear History", use_container_width=True):
            st.session_state.query_history = []
            st.session_state.generated_sql = None
            st.session_state.raw_response = None
            st.session_state.current_question = None
            st.rerun()

    if generate_button and user_question:
        q = user_question.strip()
        st.session_state.current_question = q
        st.session_state.generated_sql = None
        st.session_state.raw_response = None

        with st.spinner("🤖 AI is generating SQL..."):
            sql_query, raw = generate_sql_with_gpt(q)

        if sql_query:
            st.session_state.generated_sql = sql_query
            st.session_state.raw_response = raw
            st.success("✅ Query generated successfully!")
        else:
            st.error("⚠️ No SQL query found in model response or an error occurred.")

    # Review + Run (persist after rerun)
    if st.session_state.generated_sql:
        st.markdown("---")
        st.subheader("🧾 Review and Run Query")
        st.info(f"**Question:** {st.session_state.current_question}")

        edited_sql = st.text_area(
            "Review and edit the SQL query if needed:",
            value=st.session_state.generated_sql,
            height=220,
            key=f"sql_editor_{st.session_state.current_question}",
        )

        run_button = st.button("▶️ Run Query", type="primary", use_container_width=True)
        if run_button:
            user_sql = (edited_sql or "").strip()
            if not user_sql:
                st.warning("No SQL to run.")
            else:
                first = user_sql.split(None, 1)[0].lower()
                if first not in ("select", "with"):
                    st.error()
                else:
                    try:
                        with st.spinner("Executing query..."):
                            df = pg_query(user_sql)
                        st.session_state.query_history.append({
                            "question": st.session_state.current_question,
                            "sql": user_sql,
                            "rows": int(len(df)),
                        })
                        st.markdown("---")
                        st.subheader("Query Results")
                        st.success(f"✅ Query returned {len(df)} rows")
                        st.dataframe(df, use_container_width=True)
                    except Exception as e:
                        st.error("SQL error:")
                        st.exception(e)

        with st.expander("Show model response (debug)", expanded=False):
            st.code(st.session_state.raw_response or "", language="text")

    # Query history (like first app)
    if st.session_state.query_history:
        st.markdown("---")
        st.subheader("🕘 Query History")

        # show last 5
        recent = list(reversed(st.session_state.query_history[-5:]))
        total = len(st.session_state.query_history)

        for idx, item in enumerate(recent):
            num = total - idx
            qtxt = (item.get("question") or "")[:60]
            with st.expander(f"Query {num}: {qtxt}..."):
                st.markdown(f"**Question:** {item.get('question','')}")
                st.code(item.get("sql", ""), language="sql")
                st.caption(f"Returned {item.get('rows', 0)} rows")

                if st.button("Re-run this query", key=f"rerun_{num}", use_container_width=True):
                    try:
                        with st.spinner("Executing query..."):
                            df = pg_query(item.get("sql", ""))
                        st.success(f"✅ Query returned {len(df)} rows")
                        st.dataframe(df, use_container_width=True)
                    except Exception as e:
                        st.error("SQL error:")
                        st.exception(e)

    # Optional: Custom SQL section (kept, but styled like the first app)
    st.markdown("---")
    st.subheader("🏃🏽‍➡️ Run custom SQL")
    custom = st.text_area("Custom SQL", height=140, placeholder="SELECT * FROM raw_data LIMIT 10;")
    if st.button("Run custom SQL", use_container_width=True):
        s = (custom or "").strip()
        if not s:
            st.warning("No SQL.")
        else:
            first = s.split(None, 1)[0].lower()
            if first not in ("select", "with"):
                st.error("Only SELECT/WITH queries allowed.")
            else:
                try:
                    with st.spinner("Executing query..."):
                        df = pg_query(s)
                    st.success(f"Returned {len(df)} rows")
                    st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error("SQL error:")
                    st.exception(e)

if __name__ == "__main__":
    main()
