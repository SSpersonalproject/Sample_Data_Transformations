# Sample_Data_Transformations

This project takes a sample transaction dataset, and transforms it into presentable tables
ready for analysis in a DuckDB database named sales_db. 
- Orchestration is managed locally, via modular python calls.
- Transformation is handled via pandas.
- pandas dataframes are then inserted directly into modelled tables.


## Setup 
- Run the following commands in the terminal
  - pip install -r requirements.txt
  - python -m orchestrate.load_transactions_data


## Directories
Below, I will break down each directory and what it does

### Analysis
This folder contains two sets of queries and corresponding outputs. The first
query calculates the top 3 SKUs by month. The second calculates the inventory value
at the end of each month

### DDL
This folder stores DDL commands used to create schemas and objects in the database.
There is a db_setup.py file with commands to create schemas. Each schema has its own directory,
with DDL to create the objects in the schema.

### Docs
Data modelling files, split out by schema. Each table has a markdown file containing key info,
and a CSV containing column level details.

### Ingest
This folder contains python files that build out key functions used to ingest data
from source file into a staging dataframe.

### Load
This folder contains python files that load data from dataframes into tables in the duckDB
database.

### Orchestrate 
This folder houses load_transactions_data.py, which can be called to run the pipeline.

### Transform
This folder contains python files that transform the data from the staging dataframe
into bespoke dataframes for each table.
It also contains data quality functions that can be called on any dataframe.

## Assumptions and Trade-Offs

### Assumptions
- sku_code and product.category_id are unique across different brands
- source data will be received on a regular basis and will need to appended to existing data
  without duplication
- region.id is unique across different countries

### Technology Trade-Offs

- Orchestration is kept simple and local, via modular python calls. This is to keep the project
  lightweight, and easy to run, assuming it only needs to handle files on an ad hoc basis.
- Similarly, DuckDB is used as the database to keep things lightweight and easy to run.
- In another section below, I will list out suggested improvements for is this
  were to be productionised.

### Data Model Trade-Offs

I would define the current data model as 'moderately normalised'. This brings several advantages:
- The data is split into logical dimensions, making it easier to maintain and update over time
- Normalisation reduces data redundancy, ensuring consistency
- Scalability: If we need to scale up the database, it will be easy to add new dimensions

It also comes with disadvantages:
- Increased query complexity due to necessity of joins
- The above comes with a performance overhead
- The ETL process is more complex, as data needs to be transformed and loaded into multiple tables

The chosen level of normalisation strikes a balance between these factors, making it suitable
for the current use case while allowing for future growth and complexity. Were this database to be 
scaled up significantly, I would recommend further normalisation. 

## Suggested Improvements for Productionisation
- Orchestration: Move to a more robust orchestration tool such as Dagster or Airflow to handle
  scheduling, monitoring, and error handling.
- Database: Move to a more scalable database solution such as Snowflake or databricks to handle
  larger datasets and concurrent users.
  - Further to this point, DuckDB does not currently support role based access control, multiple
    warehouse sizes, or clustering. Moving to a database such as Snowflake would offer all these things
    and more, making it much more suitable for production use.
- Shift from an ETL to ELT pattern
  - The project specified transformations via pandas
  - However, in a production environment, it would be more efficient to perform transformations
    within the database itself, leveraging its processing power and reducing data movement.
  - This would require a new data model. I would recommend medallion architecture to allow for
    incremental data loads, data quality checks, and easier debugging.
  - This would allow us to use dbt, which would bring several further benefits:
    - Modular SQL development
    - Automated testing and documentation
    - Easier deployment and scheduling
    - Lineage and impact analysis
    - Performance optimization
- Create separate repositories to manage different aspects of the project:
  - Infrastructure as Code (IaC) repo to manage database and cloud infrastructure
  - Data pipeline repo to manage ETL/ELT code, orchestration, testing and DQ.
  - Data model repo to manage DDL version control

## Analysis - Finance Datamart

Please see the analysis folder for queries and outputs. Here, I will share a summary of 
analysis undertaken.


### a) What are the top 3 SKUs by realised profit each month?

Please see analysis/month_end_inv to see a full analysis by SKU code and month. The query in
analysis/month_end.py can be modified and re-ran if you desire to modify the output. 

Some key takeaways: 
- SKUs for Toasted Oak barrels are consistently among the highest by profit 
- The highest performing SKUs are mostly of the corn or rye grain variety. 
- Whiskey SKUs generally are the most profitable
- Buffalo Trace, Macallan and Makers mark consistently have top performing SKUs by profit

Recommendations: 
- Focus on Whiskey as this seems to be where the most profit is
  - Focus on corn and rye grain varieties
- Prioritise relationship with Buffalo Trace, Macallan and Makers Mark as 
  these are profitable brands.





