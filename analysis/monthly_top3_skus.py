import duckdb
from constants import DB_PATH


# This query checks how many distinct courses there are.

conn = duckdb.connect(str(DB_PATH))
result = conn.execute(
"""
    WITH MONTHLY_PROFIT AS (
SELECT
    sku_code,
    date_part('year', TXN_DATE) as year,
    date_part('month', TXN_DATE) AS month,
    SUM(REALIZED_PROFIT) AS MONTHLY_REALIZED_PROFIT,
    ROW_NUMBER() OVER (
            PARTITION BY date_part('year', TXN_DATE), date_part('month', TXN_DATE)
            ORDER BY SUM(REALIZED_PROFIT) DESC
        ) AS rank
FROM TRANSACTION.TRANSACTIONS
GROUP BY sku_code, year, month
ORDER BY year, month, MONTHLY_REALIZED_PROFIT DESC
)
SELECT
    sku_code,
    year,
    month,
    MONTHLY_REALIZED_PROFIT
FROM MONTHLY_PROFIT
WHERE rank <= 3
ORDER BY year, month, MONTHLY_REALIZED_PROFIT DESC
"""
).fetchall()
print("Output:", result)


