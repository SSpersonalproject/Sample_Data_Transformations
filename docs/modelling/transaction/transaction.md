# Transactions Table

The transactions table provides detailed information about individual transactions, including timestamps, product details, and financial metrics.

## Schema

- The schema for this table is TRANSACTION

## Data Source

- The source for this data is the transaction_data_sample file, found in the source_data directory.

## Primary Key

- The composite Primary Key for transactions is TIMESTAMP, SKU_CODE, and TRANSACTION_TYPE

## ETL Process
1. Using Pandas, convert transaction_data_sample.csv to a dataframe.
2. Select the required columns: TIMESTAMP, TXN_DATE, SKU_CODE, TRANSACTION_TYPE, QUANTITY, UNIT_PRICE, REVENUE, COST, and REALIZED_PROFIT.
3. Insert into the TRANSACTION.TRANSACTIONS table, hardcoding the current date as LOAD_TS and 'TRANSACTIONS_FILE' as SOURCE_CD.

## Notes

- Please review transactions.csv for column-level mapping details.