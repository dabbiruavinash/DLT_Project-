# schemas/banking_schemas.py

from pyspark.sql.types import *

# Bronze Layer Schemas (Raw CDC Data)
bronze_customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("updated_date", TimestampType(), True),
    StructField("op_type", StringType(), True),  # CDC operation type
    StructField("op_ts", TimestampType(), True),  # Operation timestamp
    StructField("source_ts", TimestampType(), True),  # Source timestamp
    StructField("_rescued_data", StringType(), True)
])

bronze_account_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("account_type", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("balance", DecimalType(15, 2), True),
    StructField("status", StringType(), True),
    StructField("opened_date", DateType(), True),
    StructField("closed_date", DateType(), True),
    StructField("op_type", StringType(), True),
    StructField("op_ts", TimestampType(), True),
    StructField("source_ts", TimestampType(), True),
    StructField("_rescued_data", StringType(), True)
])

bronze_transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DecimalType(15, 2), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("description", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("status", StringType(), True),
    StructField("op_type", StringType(), True),
    StructField("op_ts", TimestampType(), True),
    StructField("source_ts", TimestampType(), True),
    StructField("_rescued_data", StringType(), True)
])

# Silver Layer Schemas (Cleaned Data)
silver_customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("created_date", TimestampType(), False),
    StructField("updated_date", TimestampType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("cdc_timestamp", TimestampType(), False)
])

silver_account_schema = StructType([
    StructField("account_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("account_type", StringType(), False),
    StructField("account_number", StringType(), False),
    StructField("balance", DecimalType(15, 2), False),
    StructField("status", StringType(), False),
    StructField("opened_date", DateType(), False),
    StructField("closed_date", DateType(), True),
    StructField("cdc_timestamp", TimestampType(), False)
])

silver_transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("account_id", IntegerType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", DecimalType(15, 2), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("description", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("status", StringType(), False),
    StructField("cdc_timestamp", TimestampType(), False)
])