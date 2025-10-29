import duckdb
from constants import DB_PATH
conn = duckdb.connect(str(DB_PATH))

conn.execute(
"""
    CREATE OR REPLACE TABLE PRODUCT.BRANDS
    ( BRAND_ID VARCHAR,
      BRAND_NAME VARCHAR,
      SOURCE_CD VARCHAR,
      LOAD_TS TIMESTAMP )
"""
)