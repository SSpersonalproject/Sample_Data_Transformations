import pandas as pd
from constants import DB_PATH
from transform.product.brands import create_brands

def insert_brands_data(brands_df, conn):
    """
    Inserts brand dimension data into the 'PRODUCT.BRANDS' table in the sales_db database.
    """
    # Delete existing rows that match on the primary key
    conn.execute("""
        DELETE FROM PRODUCT.BRANDS
        WHERE brand_id IN (SELECT CAST(brand_id AS VARCHAR) FROM brands_df)
    """)

    # Insert new rows
    conn.execute("""
        INSERT INTO PRODUCT.BRANDS (brand_id, brand_name, SOURCE_CD, LOAD_TS)
        SELECT brand_id, brand_name, 'TRANSACTIONS_FILE' as SOURCE_CD, CURRENT_TIMESTAMP as LOAD_TS
        FROM brands_df
    """)
    return