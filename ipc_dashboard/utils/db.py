import os
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


@st.cache_resource
def get_engine():
    pw  = quote_plus(os.getenv("PG_PASSWORD", ""))
    url = (
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{pw}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '25060')}"
        f"/{os.getenv('PG_DB', 'PROD_ANALYTICS_DB')}?sslmode=require"
    )
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=3600, show_spinner="Loading…")
def run_query(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)
