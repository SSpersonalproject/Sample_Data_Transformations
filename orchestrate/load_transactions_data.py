import duckdb
from constants import SOURCE_PATH, DB_PATH
from ingest.ingest_functions import poke_for_files, get_matching_file_path, create_dataframe_from_csv, rename_dataframe_cols

from ingest.transactions.ingest_transactions import stage_transactions_data
from transform.location.regions import create_regions
from transform.product.brands import create_brands
from transform.product.product_categories import create_product_categories
from transform.product.products import create_products
from transform.transaction.transactions import create_transactions

from transform.dq_functions import check_uniqueness, check_for_nulls

from load.location.regions import insert_regions_data
from load.product.brands import insert_brands_data
from load.product.product_categories import insert_product_categories_data
from load.product.products import insert_products_data
from load.transaction.transactions import insert_transactions_data

conn = duckdb.connect(str(DB_PATH))


def create_dataframes():
    stage_transactions = stage_transactions_data()

    regions = create_regions(stage_transactions)

    brands = create_brands(stage_transactions)
    product_categories = create_product_categories(stage_transactions)
    products = create_products(stage_transactions)

    transactions = create_transactions(stage_transactions)

    return [regions, brands, product_categories, products, transactions]

def run_dq_checks(dataframes):

    regions, brands, product_categories, products, transactions = dataframes

    check_for_nulls(regions, ["region_id", "region_name", "region_country_id", "region_country_name"], 'regions')
    check_uniqueness(regions, ["region_id"])

    check_for_nulls(brands, ["brand_id", "brand_name"], 'brands')
    check_uniqueness(brands, ["brand_id"])

    check_for_nulls(product_categories, ["product_category_id", "product_category_name"], 'product_categories')
    check_uniqueness(product_categories, ["product_category_id"])

    check_for_nulls(products, ["sku_code", "brand_id", "product_category_id"], 'products')
    check_uniqueness(products, ["sku_code"])

    check_for_nulls(transactions, ["timestamp", "sku_code", "transaction_type", "quantity", "unit_price", "revenue", "cost", "realized_profit"], 'transactions')
    check_uniqueness(transactions, ["timestamp", "sku_code", "transaction_type"])

    return

def insert_into_db(dataframes):
    regions, brands, product_categories, products, transactions = dataframes

    insert_regions_data(regions, conn)
    insert_brands_data(brands, conn)
    insert_product_categories_data(product_categories, conn)
    insert_products_data(products, conn)
    insert_transactions_data(transactions, conn)

    return


poke_for_files(SOURCE_PATH, r"(transaction_data.*\.csv$)")
dfs = create_dataframes()
run_dq_checks(dfs)
insert_into_db(dfs)



