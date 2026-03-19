import requests
import pandas as pd
import snowflake.connector
import os, uuid
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── Load environment variables from .env FIRST ───────────────────
load_dotenv()

# ── The 8 FRED series we are pulling ─────────────────────────────
SERIES = {
    "DRCLACBS":      "Delinquency Rate - Consumer Loans",
    "DRBLACBS":      "Delinquency Rate - Business Loans",
    "DRCCLACBS":     "Delinquency Rate - Credit Cards",
    "DRCRELEXFACBS": "Delinquency Rate - Commercial Real Estate",
    "DRSFRMACBS":    "Delinquency Rate - Single Family Mortgages",
    "CORBLACBS":     "Charge-Off Rate - Business Loans",
    "CORCACBS":     "Charge-Off Rate - Credit Cards",
    "FEDFUNDS":      "Federal Funds Rate",
}

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


# ── STEP 1: FETCH ONE SERIES FROM FRED ───────────────────────────
def fetch_series(series_id, series_name, api_key):
    print(f"Fetching {series_id}...")
    params = {
        "series_id":         series_id,
        "api_key":           api_key,
        "file_type":         "json",
        "observation_start": "2000-01-01",
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    observations = response.json()["observations"]
    rows = []
    for obs in observations:
        rows.append({
            "SERIES_ID":        series_id,
            "SERIES_NAME":      series_name,
            "OBSERVATION_DATE": obs["date"],
            "VALUE":            obs["value"],
            "UNITS":            "Percent",
            "FREQUENCY":        "Quarterly",
            "LOADED_AT":        datetime.now(timezone.utc).isoformat(),
            "LOAD_BATCH_ID":    str(uuid.uuid4())[:8],
        })
    print(f"  -> {len(rows)} observations fetched")
    return rows


# ── STEP 2: FETCH ALL 8 SERIES ────────────────────────────────────
def fetch_all_series(api_key):
    all_rows = []
    for series_id, series_name in SERIES.items():
        all_rows.extend(fetch_series(series_id, series_name, api_key))
    df = pd.DataFrame(all_rows)
    print(f"\nTotal rows fetched: {len(df)}")
    return df


# ── STEP 3: SAVE CSV LOCALLY ──────────────────────────────────────
def save_csv(df):
    path = "data/raw/fred_loans_raw.csv"
    os.makedirs("data/raw", exist_ok=True)
    df.to_csv(path, index=False)
    print(f"CSV saved: {path} ({len(df)} rows)")
    return path


# ── STEP 4: UPLOAD TO SNOWFLAKE INTERNAL STAGE ───────────────────
def load_to_snowflake(csv_path):
    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
    )
    cur = conn.cursor()
    print("  -> Connected successfully")

    abs_path = os.path.abspath(csv_path)
    put_sql  = f"PUT file://{abs_path} @LOANLENS_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    print("Uploading CSV to Snowflake Internal Stage...")
    cur.execute(put_sql)
    print("  -> File uploaded to @LOANLENS_STAGE")

    print("Loading data into FRED_DELINQUENCY_RAW...")
    cur.execute("""
        COPY INTO FRED_DELINQUENCY_RAW
        FROM @LOANLENS_STAGE/fred_loans_raw.csv.gz
        FILE_FORMAT = (
            TYPE                         = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER                  = 1
            NULL_IF                      = ('', '.', 'NULL')
            EMPTY_FIELD_AS_NULL          = TRUE
        )
        ON_ERROR = 'CONTINUE'
        PURGE    = TRUE
    """)
    results = cur.fetchall()
    for row in results:
        print(f"  -> {row}")

    conn.commit()
    print("Data committed to Snowflake.")
    cur.close()
    conn.close()


# ── STEP 5: RUN THE FULL PIPELINE ────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  LoanLens - FRED Ingestion Pipeline")
    print("=" * 55)

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY not found. Check your .env file.")

    df       = fetch_all_series(api_key)
    csv_path = save_csv(df)
    load_to_snowflake(csv_path)

    print("\n" + "=" * 55)
    print("  Pipeline complete.")
    print("=" * 55)