import os, hashlib
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def make_id(company: str, date: str, amount_text: str):
    key = f"{company}|{date}|{amount_text or ''}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )

def upsert(df: pd.DataFrame, table: str):
    # Build required columns and types
    from datetime import datetime
    df = df.copy()
    df["id"] = [make_id(c or "", d or "", a or "") for c, d, a in zip(df["company"], df["date"], df["amount_text"])]
    df["created_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = get_conn()
    try:
        temp = table + "_STAGE_" + os.urandom(3).hex()
        cur = conn.cursor()
        cur.execute(f"create temporary table {temp} like {table}")
        # Order columns to match table
        cols = ["ID","YEAR","DATE","COMPANY","SERIES","AMOUNT_VALUE","AMOUNT_CURRENCY","AMOUNT_TEXT",
                "INVESTORS","BLURB","SOURCE_TYPE","SOURCE_URL","TRACKER_URL","CREATED_AT"]
        df_load = pd.DataFrame({
            "ID": df["id"],
            "YEAR": df["year"].astype("Int64"),
            "DATE": pd.to_datetime(df["date"]).dt.date,
            "COMPANY": df["company"],
            "SERIES": df["series"],
            "AMOUNT_VALUE": pd.to_numeric(df["amount_value"]),
            "AMOUNT_CURRENCY": df["amount_currency"],
            "AMOUNT_TEXT": df["amount_text"],
            "INVESTORS": df["investors"],
            "BLURB": df["blurb"],
            "SOURCE_TYPE": df["source_type"],
            "SOURCE_URL": df["source_url"],
            "TRACKER_URL": df["tracker_url"],
            "CREATED_AT": df["created_at"]
        })
        write_pandas(conn, df_load, temp, auto_create_table=False, quote_identifiers=False)
        merge_sql = f"""
        merge into {table} t
        using {temp} s
          on t.ID = s.ID
        when matched then update set
          YEAR=s.YEAR, DATE=s.DATE, COMPANY=s.COMPANY, SERIES=s.SERIES,
          AMOUNT_VALUE=s.AMOUNT_VALUE, AMOUNT_CURRENCY=s.AMOUNT_CURRENCY, AMOUNT_TEXT=s.AMOUNT_TEXT,
          INVESTORS=s.INVESTORS, BLURB=s.BLURB, SOURCE_TYPE=s.SOURCE_TYPE, SOURCE_URL=s.SOURCE_URL,
          TRACKER_URL=s.TRACKER_URL, UPDATED_AT=current_timestamp()
        when not matched then insert (
          ID, YEAR, DATE, COMPANY, SERIES, AMOUNT_VALUE, AMOUNT_CURRENCY, AMOUNT_TEXT,
          INVESTORS, BLURB, SOURCE_TYPE, SOURCE_URL, TRACKER_URL, CREATED_AT
        ) values (
          s.ID, s.YEAR, s.DATE, s.COMPANY, s.SERIES, s.AMOUNT_VALUE, s.AMOUNT_CURRENCY, s.AMOUNT_TEXT,
          s.INVESTORS, s.BLURB, s.SOURCE_TYPE, s.SOURCE_URL, s.TRACKER_URL, s.CREATED_AT
        );
        """
        cur.execute(merge_sql)
    finally:
        conn.close()
