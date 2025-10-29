import pandas as pd
from constants import DB_PATH
from transform.location.regions import create_regions

def insert_regions_data(location__region, conn):
    """
    Inserts region dimension data into the 'LOCATION.REGIONS' table in the sales_db database.
    """
    # Delete existing rows that match on the primary key
    conn.execute("""
                DELETE FROM LOCATION.REGIONS
                WHERE region_id IN (SELECT CAST(region_id as VARCHAR) FROM location__region)
            """)

    conn.execute("""
        INSERT INTO LOCATION.REGIONS (region_id, region_name, region_country_id, region_country_name, SOURCE_CD, LOAD_TS)
        SELECT region_id, region_name, region_country_id, region_country_name, 'TRANSACTIONS_FILE' as SOURCE_CD, CURRENT_TIMESTAMP as LOAD_TS
        FROM location__region
    """)
    return