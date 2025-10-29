# Product Categories Table

The product categories table provides a mapping of product category IDs to their corresponding product category names.

## Schema

- The schema for this table is PRODUCT

## Data Source

- The source for this data is the transaction_data_sample file, found in the source_data directory.

## Primary Key

- The Primary Key for product categories is PRODUCT_CATEGORY_ID

## ETL Process
1. Using Pandas, convert transaction_data_sample.csv to a dataframe.
2. Select distinct PRODUCT_CATEGORY_ID and PRODUCT_CATEGORY_NAME values and store them in a new dataframe.
3. Insert into the PRODUCT.PRODUCT_CATEGORIES table, hardcoding the current date as LOAD_TS and 'TRANSACTION_FILE' as SOURCE_CD.

## Notes

- Please review product_categories.csv for column-level mapping details.