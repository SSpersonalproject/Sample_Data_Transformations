import pandas as pd
from ingest.transactions.ingest_transactions import stage_transactions_data

def create_regions(transactions_stage):
    """
    Creates a new DataFrame 'location__region' with distinct region-related columns
    from the 'transactions_stage' DataFrame.
    """
    location__region = transactions_stage[[
        "region_id",
        "region_name",
        "region_country_id",
        "region_country_name"
    ]].drop_duplicates()
    return location__region
