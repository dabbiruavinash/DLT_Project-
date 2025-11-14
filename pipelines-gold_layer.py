import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Customer Summary Gold Table
@dlt.table(
    comment="Customer summary with account aggregates",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_summary_gold():
    return (
        dlt.read("active_customers_mv")
        .join(
            dlt.read("account_silver").filter(col("status") == "ACTIVE"),
            "customer_id",
            "left"
        )
        .groupBy("customer_id", "first_name", "last_name", "email")
        .agg(
            count("account_id").alias("total_accounts"),
            sum("balance").alias("total_balance"),
            max("opened_date").alias("latest_account_open_date"),
            collect_set("account_type").alias("account_types")
        )
        .withColumn("customer_tier", 
                   when(col("total_balance") >= 100000, "PREMIUM")
                   .when(col("total_balance") >= 50000, "GOLD")
                   .when(col("total_balance") >= 10000, "SILVER")
                   .otherwise("STANDARD"))
        .withColumn("last_updated", current_timestamp())
    )

# Daily Transaction Summary
@dlt.table(
    comment="Daily transaction summary by account type",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def daily_transaction_summary_gold():
    return (
        dlt.read("transaction_analytics_v")
        .withColumn("transaction_date", to_date(col("transaction_date")))
        .groupBy("transaction_date", "account_type", "transaction_type")
        .agg(
            count("transaction_id").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("average_amount"),
            min("amount").alias("min_amount"),
            max("amount").alias("max_amount")
        )
        .withColumn("last_updated", current_timestamp())
    )

# Customer Behavior Gold Table
@dlt.table(
    comment="Customer behavior and transaction patterns",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_behavior_gold():
    transaction_stats = (
        dlt.read("transaction_analytics_v")
        .groupBy("customer_id")
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("amount").alias("total_transaction_amount"),
            avg("amount").alias("avg_transaction_amount"),
            countDistinct("transaction_type").alias("unique_transaction_types"),
            max("transaction_date").alias("last_transaction_date")
        )
    )
    
    return (
        dlt.read("customer_summary_gold")
        .join(transaction_stats, "customer_id", "left")
        .withColumn("transaction_frequency",
                   when(col("total_transactions") / datediff(current_date(), col("last_transaction_date")) > 1, "HIGH")
                   .when(col("total_transactions") / datediff(current_date(), col("last_transaction_date")) > 0.5, "MEDIUM")
                   .otherwise("LOW"))
        .withColumn("last_updated", current_timestamp())
    )