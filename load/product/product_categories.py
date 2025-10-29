import pandas as pd
from constants import DB_PATH
from transform.product.product_categories import create_product_categories

def insert_product_categories_data(product_categories_df, conn):
    """
    Inserts product category dimension data into the 'PRODUCT.PRODUCT_CATEGORIES' table in the sales_db database.
    """
    # Delete existing rows that match on the primary key
    conn.execute("""
        DELETE FROM PRODUCT.PRODUCT_CATEGORIES
        WHERE product_category_id IN (SELECT CAST(product_category_id AS VARCHAR) FROM product_categories_df)
    """)



    # Insert new rows
    conn.execute("""
        INSERT INTO PRODUCT.PRODUCT_CATEGORIES (product_category_id, product_category_name, SOURCE_CD, LOAD_TS)
        SELECT product_category_id, product_category_name, 'TRANSACTIONS_FILE' as SOURCE_CD, CURRENT_TIMESTAMP as LOAD_TS
        FROM product_categories_df
    """)
    return