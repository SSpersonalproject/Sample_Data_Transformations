#Ingest Transactions data into a single Pandas dataframe, rename columns appropriately and
#create a date and load ts columns

import pandas as pd
import os
from constants import SOURCE_PATH
from ingest.ingest_functions import get_matching_file_path, create_dataframe_from_csv, rename_dataframe_cols



def stage_transactions_data():
    """
    This function stages the transactions data into a dataframe,
    renames columns, and adds txn_date and record_arrival_ts columns.
    """
    txn_file_pattern = r"(transaction_data.*\.csv$)"
    txn_file_name = get_matching_file_path(SOURCE_PATH, txn_file_pattern)

    #Stage transactions data in a dataframe
    transactions_stage = create_dataframe_from_csv(txn_file_name)

    #Rename columns
    transactions_stage = rename_dataframe_cols(transactions_stage,
                                               ["row_num",
                                                "timestamp",
                                                "sku_code",
                                                "transaction_type",
                                                "quantity",
                                                "unit_price",
                                                "revenue",
                                                "cost",
                                                "realized_profit",
                                                "id",
                                                "code",
                                                "product_category_id",
                                                "product_category_name",
                                                "region_id",
                                                "region_name",
                                                "region_country_id",
                                                "region_country_name",
                                                "sub_varietal_id",
                                                "sub_varietal_name",
                                                "brand_id",
                                                "brand_name",
                                                "attribute_abv",
                                                "attribute_bottle_ml",
                                                "attribute_year",
                                                "attribute_age",
                                                "attribute_grape_variety",
                                                "attribute_grain_variety",
                                                "attribute_blend",
                                                "attribute_barrel_type"] )
    # Add a txn_date column
    transactions_stage.insert(
        transactions_stage.columns.get_loc("timestamp") + 1,  # Next to the timestamp column
        "txn_date",
        pd.to_datetime(transactions_stage["timestamp"], dayfirst=True).dt.date  # Extract date from the timestamp
    )

    # Create a record_arrival_ts column with the current timestamp
    transactions_stage["record_arrival_ts"] = pd.Timestamp.now()
    return transactions_stage

#stage_transactions_data()




