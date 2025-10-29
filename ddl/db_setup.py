import duckdb
from constants import DB_PATH
conn = duckdb.connect(str(DB_PATH))



# Setup the database schemas
conn.execute(
"""
    CREATE SCHEMA LOCATION
"""
)

conn.execute(
"""
    CREATE SCHEMA PRODUCT
"""
)

conn.execute(
"""
    CREATE SCHEMA TRANSACTION
"""
)