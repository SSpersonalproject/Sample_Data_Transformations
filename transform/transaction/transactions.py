import pandas as pd
from ingest.transactions.ingest_transactions import stage_transactions_data
from transform.dq_functions import check_uniqueness, check_for_nulls

def create_transactions(transactions_stage):
    """
    Creates a new DataFrame 'transactions' with the specified transaction-related columns
    from the 'transactions_stage' DataFrame.
    """
    # Select the required columns
    transactions = transactions_stage[[
        "timestamp",
        "txn_date",
        "sku_code",
        "transaction_type",
        "quantity",
        "unit_price",
        "revenue",
        "cost",
        "realized_profit"
    ]]

    transactions['timestamp'] = pd.to_datetime(transactions['timestamp'], dayfirst=True)

    transactions['timestamp'] = transactions['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return transactions


