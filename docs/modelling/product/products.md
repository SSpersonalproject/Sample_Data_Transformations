# Products Table

The products table provides detailed information about products, including their categories, brands, and attributes.

## Schema

- The schema for this table is PRODUCT

## Data Source

- The source for this data is the transaction_data_sample file, found in the source_data directory.

## Primary Key

- The Primary Key for products is SKU_CODE

## ETL Process
1. Using Pandas, convert transaction_data_sample.csv to a dataframe.
2. Select distinct SKU_CODE, PRODUCT_CATEGORY_ID, BRAND_ID, and other product-related columns, and store them in a new dataframe.
3. Insert into the PRODUCT.PRODUCTS table, hardcoding the current date as LOAD_TS and 'TRANSACTION_FILE' as SOURCE_CD.

## Notes

- Please review products.csv for column-level mapping details.