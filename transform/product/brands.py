import pandas as pd
from ingest.transactions.ingest_transactions import stage_transactions_data

def create_brands(transactions_stage):
    """
    Creates a new DataFrame 'brands' with distinct brand-related columns
    from the 'transactions_stage' DataFrame.
    """
    brands = transactions_stage[[
        "brand_id",
        "brand_name"
    ]].drop_duplicates()
    return brands


