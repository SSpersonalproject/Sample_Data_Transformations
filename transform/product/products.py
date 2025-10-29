import pandas as pd
from ingest.transactions.ingest_transactions import stage_transactions_data

def create_products(transactions_stage):
    """
    Creates a new DataFrame 'products' with distinct product-related columns
    from the 'transactions_stage' DataFrame and removes the 'attribute_' prefix
    from column names.
    """
    # Select the required columns
    products = transactions_stage[[
        "sku_code",
        "product_category_id",
        "brand_id",
        "sub_varietal_name",
        "attribute_abv",
        "attribute_bottle_ml",
        "attribute_year",
        "attribute_age",
        "attribute_grape_variety",
        "attribute_grain_variety",
        "attribute_blend",
        "attribute_barrel_type"
    ]].drop_duplicates()

    # Rename columns to remove the 'attribute_' prefix
    products = products.rename(columns=lambda col: col.replace("attribute_", "") if col.startswith("attribute_") else col)

    return products




