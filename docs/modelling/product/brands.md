# Brands Table

The brands table provides a mapping of brand IDs to their corresponding brand names.

## Schema

- The schema for this table is PRODUCT

## Data Source

- The source for this data is the transaction_data_sample file, found in the source_data directory.

## Primary Key

- The Primary Key for brands is BRAND_ID

## ETL Process
1. Using Pandas, convert transaction_data_sample.csv to a dataframe.
2. Select distinct BRAND_ID and BRAND_NAME values and store them in a new dataframe.
3. Insert into the PRODUCT.BRANDS table, hardcoding the current date as LOAD_TS and 'TRANSACTION_FILE' as SOURCE_CD.

## Notes

- Please review brands.csv for column-level mapping details.