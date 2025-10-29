import pandas as pd
from ingest.transactions.ingest_transactions import stage_transactions_data

def create_product_categories(transactions_stage):
    """
    Creates a new DataFrame 'product_categories' with distinct product category-related columns
    from the 'transactions_stage' DataFrame.
    """
    product_categories = transactions_stage[[
        "product_category_id",
        "product_category_name"
    ]].drop_duplicates()
    return product_categories

