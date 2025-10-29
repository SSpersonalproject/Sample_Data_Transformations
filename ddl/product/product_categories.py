import duckdb
from constants import DB_PATH
conn = duckdb.connect(str(DB_PATH))

conn.execute(
"""
    CREATE OR REPLACE TABLE PRODUCT.PRODUCT_CATEGORIES
    ( PRODUCT_CATEGORY_ID VARCHAR,
      PRODUCT_CATEGORY_NAME VARCHAR,
      SOURCE_CD VARCHAR,
      LOAD_TS TIMESTAMP )
"""
)