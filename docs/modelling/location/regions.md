# Regions Table

The regions table provides a mapping of region codes to their corresponding region names. 

## Schema

- The schema for this table is LOCATION

## Data Source

- The source for this data is the transaction_data_sample file, found in source_data directory.

## Primary Key

- The Primary Key for regions is REGION_CD

## ETL process
1. Using Pandas, convert transaction_data_sample.csv to a dataframe
3. Select distinct REGION_CD and REGION_NAME values and store in a new dataframe
4. Insert into the LOCATION.REGIONS table, hardcoding the current date as LOAD_TS
   and 'TRANSACTION_FILE' as SOURCE_CD.

## Notes

- Please review regions.csv for column level mapping details