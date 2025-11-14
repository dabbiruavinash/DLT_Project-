# pipelines/main_pipeline.py

import dlt
from pyspark.sql.functions import *

# Main pipeline flow with Auto CDC
@dlt.create_auto_cdc_flow(
    name="banking_cdc_flow",
    source_datasets=["customer_bronze_clean_v", "account_bronze_clean_v", "transaction_bronze_clean_v"],
    target_datasets=["customer_silver", "account_silver", "transaction_silver"]
)
def banking_cdc_flow():
    # This creates an automatic CDC flow between bronze and silver layers
    pass

# Append flow for batch operations
@dlt.append_flow(
    target='customer_summary_gold',
    source='active_customers_mv'
)
def update_customer_summary():
    return dlt.read("active_customers_mv")

# Append flow for transaction aggregations
@dlt.append_flow(
    target='daily_transaction_summary_gold',
    source='transaction_analytics_v'
)
def update_transaction_summary():
    return (
        dlt.read("transaction_analytics_v")
        .withColumn("transaction_date", to_date(col("transaction_date")))
    )