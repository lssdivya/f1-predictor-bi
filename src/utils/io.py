import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PG_URL = os.getenv("POSTGRES_URL")
engine = create_engine(PG_URL, pool_pre_ping=True)

def upsert_df(df, table_name, schema="raw", pk_cols=None):
    with engine.begin() as conn:
        tmp_table = f"{table_name}_stg"
        df.to_sql(tmp_table, conn, schema=schema, if_exists="replace", index=False)
        if not pk_cols:
            conn.exec_driver_sql(
                f"insert into {schema}.{table_name} select * from {schema}.{tmp_table}"
            )
        else:
            cols = ",".join(df.columns)
            updates = ",".join([f"{c}=excluded.{c}" for c in df.columns if c not in pk_cols])
            conn.exec_driver_sql(f"""
                insert into {schema}.{table_name} ({cols})
                select {cols} from {schema}.{tmp_table}
                on conflict ({','.join(pk_cols)}) do update set {updates}
            """)
        conn.exec_driver_sql(f"drop table if exists {schema}.{tmp_table}")

def fetch_df(sql):
    import pandas as pd
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn)