import duckdb
from constants import DB_PATH
conn = duckdb.connect(str(DB_PATH))

conn.execute(
"""
    CREATE OR REPLACE TABLE LOCATION.REGIONS
    ( REGION_ID VARCHAR,
      REGION_NAME VARCHAR,
      REGION_COUNTRY_ID VARCHAR,
      REGION_COUNTRY_NAME VARCHAR,
      SOURCE_CD VARCHAR,
      LOAD_TS TIMESTAMP )
"""
)