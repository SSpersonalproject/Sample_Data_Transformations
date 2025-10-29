import pandas as pd
from constants import DB_PATH
from transform.product.products import create_products

def insert_products_data(products_df, conn):
    """
    Inserts product dimension data into the 'PRODUCT.PRODUCTS' table in the sales_db database.
    """
    # Delete existing rows that match on the primary key
    conn.execute("""
        DELETE FROM PRODUCT.PRODUCTS
        WHERE sku_code IN (SELECT sku_code FROM products_df)
    """)

    # Insert new rows directly from the DataFrame
    conn.execute("""
        INSERT INTO PRODUCT.PRODUCTS (
            sku_code, product_category_id, brand_id, sub_varietal_name,
            abv, bottle_ml, year, age, grape_variety, grain_variety,
            blend, barrel_type, SOURCE_CD, LOAD_TS
        )
        SELECT
            sku_code, product_category_id, brand_id, sub_varietal_name,
            abv, bottle_ml, year, age, grape_variety, grain_variety,
            blend, barrel_type, 'TRANSACTIONS_FILE' as SOURCE_CD, CURRENT_TIMESTAMP as LOAD_TS
        FROM products_df
    """)
    return