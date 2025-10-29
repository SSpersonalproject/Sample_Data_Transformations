import pandas as pd
from constants import DB_PATH
from transform.transaction.transactions import create_transactions
def insert_transactions_data(transactions_df, conn):
    """
    Inserts transaction data into the 'TRANSACTION.TRANSACTIONS' table in the sales_db database.
    """
    # Delete existing rows that match on the composite primary key
    conn.execute("""
        DELETE FROM TRANSACTION.TRANSACTIONS
        WHERE EXISTS (
            SELECT 1
            FROM transactions_df
            WHERE TRANSACTION.TRANSACTIONS.timestamp = transactions_df.timestamp
              AND TRANSACTION.TRANSACTIONS.sku_code = transactions_df.sku_code
              AND TRANSACTION.TRANSACTIONS.transaction_type = transactions_df.transaction_type
        )
    """)

    # Insert new rows directly from the DataFrame
    conn.execute("""
        INSERT INTO TRANSACTION.TRANSACTIONS (
            timestamp, txn_date, sku_code, transaction_type, quantity, unit_price,
            revenue, cost, realized_profit, SOURCE_CD, LOAD_TS
        )
        SELECT
            timestamp, txn_date, sku_code, transaction_type, quantity, unit_price,
            revenue, cost, realized_profit, 'TRANSACTIONS_FILE' as SOURCE_CD, CURRENT_TIMESTAMP as LOAD_TS
        FROM transactions_df
    """)
    return