import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Apply Changes for Customer SCD Type 2
@dlt.table(
    comment="Customer silver table with SCD Type 2 and data quality",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
@dlt.expect_or_drop("valid_dob", "date_of_birth <= current_date()")
@dlt.expect_or_fail("adult_customers", "date_of_birth <= date_sub(current_date(), 365*18)")
def customer_silver():
    return dlt.apply_changes(
        target = "customer_silver",
        source = "customer_bronze_clean_v",
        keys = ["customer_id"],
        sequence_by = col("op_ts"),
        apply_as_deletes = expr("op_type = 'D'"),
        except_column_list = ["op_type", "_file_name", "_load_timestamp"],
        stored_as_scd_type = "2"
    )

# Materialized View for Active Customers
@dlt.table(
    comment="Materialized view of active customers",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "false"
    }
)
def active_customers_mv():
    return (
        dlt.read("customer_silver")
        .filter(col("is_active") == True)
        .select(
            "customer_id", "first_name", "last_name", "email",
            "phone", "date_of_birth", "created_date"
        )
    )

# Apply Changes for Account Data
@dlt.table(
    comment="Account silver table with current state",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def account_silver():
    return dlt.apply_changes(
        target = "account_silver",
        source = "account_bronze_clean_v",
        keys = ["account_id"],
        sequence_by = col("op_ts"),
        apply_as_deletes = expr("op_type = 'D'"),
        except_column_list = ["op_type", "_file_name", "_load_timestamp"]
    )

# Streaming Table for Real-time Transactions
@dlt.table(
    comment="Real-time transaction silver data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def transaction_silver():
    return (
        dlt.readStream("transaction_bronze_clean_v")
        .filter(col("op_type") == "I")  # Only inserts for transactions
        .withColumn("transaction_date", to_timestamp(col("transaction_date")))
        .withColumn("year", year(col("transaction_date")))
        .withColumn("month", month(col("transaction_date")))
        .withColumn("day", dayofmonth(col("transaction_date")))
        .withColumn("cdc_timestamp", current_timestamp())
        .drop("op_type", "_file_name", "_load_timestamp")
    )

# Temporary View for Transaction Analytics
@dlt.view(
    comment="Temporary view for transaction analytics"
)
def transaction_analytics_v():
    return (
        dlt.read("transaction_silver")
        .join(
            dlt.read("account_silver"),
            "account_id",
            "inner"
        )
        .join(
            dlt.read("customer_silver").filter(col("is_active") == True),
            "customer_id",
            "inner"
        )
        .select(
            "transaction_id", "account_id", "customer_id",
            "first_name", "last_name", "transaction_type",
            "amount", "transaction_date", "account_type"
        )
    )