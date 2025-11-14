import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from schema.banking_schemas import *

# configuration
source_path = "/mnt/cloudfiles/banking_data/"
checkpoint_path = "/mnt/delta/checkpoints/bronze/"
bronze_path = "/mnt/delta/bronze/"

# Autoloader for streaming Incremental Load
@dlt.table (
   comment = "Raw customer data from MySQL CDC stream",
   table_properities = {
      "quality" : "bronze",
      "pipelines.autoOptimize.managed" : "true"
    }
)

def customer_bronze_raw_full_cdc_history():
       return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation, f"{checkpoint_path}/customer_schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("mergeSchema", "true")
            .load(f"{source_path}/customers")
            .withColumn("_file_name", input_file_name())
            .withColumn("_load_timestamp", current_timestamp())
            .withColumn("_rescued_data", col("_rescued_data"))
)

@dlt.view (
   comment = "Temporary view for customer data validation"
)

def customer_bronze_validate_v():
   return (
         dlt.readStream("customer_bronze_raw_full_cdc_history")
         .select("customer_id", "first_name", "last_name", "email", "phone", "op_type", "op_ts", "_file_name"))

# cleaned Bronze Data with Expectations
@dlt.table(
   comment = "Cleaned customer bronze data with quality checks",
   table_properities = {
         "quality" : "bronze_clean",
         "pipelines.autoOptimize.managed" : "true"
   }
)

@dlt.expect("valid_customer_id", "customer_id is not null")
@dlt.expect_or_drop("valid_email", "email is not null and email like '%@%'")
@dlt.expect_or_fail("valid_timestamp", "op_ts is not null")

def customer_bronze_clean_v():
       return (
           dlt.readStream("customer_bronze_raw_full_cdc_history")
           .filter(col("op_type").isin("I","U","D"))
           .withColumn("first_name", trim(col("first_name")))
           .withColumn("last_name", trim(col("last_name")))
           .withColumn("email", lower(trim(col("email"))))
           .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", " "))
           .drop("_rescued_data")
)

#AutoLoader for account data
@dlt.table(
      comment = "Raw account data form MySQL CDC Stream",
      table_properities = {
           "quality" : "bronze",
           "pipeline.autoOptimize.managed" : "true"
     }
)

def account_bronze_raw_full_cdc_history():
       return (
           spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.schemaLocation", f"{checkpoint_path}/account_schema")
           .option("mergeSchema", "true")
           .load(f"{source_path}/accounts")
           .withColumn("_file_name", input_file_name())
           .withColumn("_load_timestamp", current_timestamp())
)

# cleaned account Bronze data
@dlt.table(
       comment = "cleaned account bronze data with quality checks",
       table_properities = {
            "quality" : "bronze_clean",
            "pipelines.autoOptimize.managed" : "true"
      }
)

@dlt.expect("valid_account_id", "account_id is not null")
@dlt.expect("valid_customer_reference", "customer_id is not null")
@dlt.expect_or_drop("valid_balance", "balance >= 0")

def account_bronze_clean_v():
    return (
        dlt.readStream("account_bronze_raw_full_cdc_history")
        .filter(col("op_type").isin("I", "U", "D"))
        .withColumn("account_number", trim(col("account_number")))
        .withColumn("account_type", upper(trim(col("account_type"))))
        .withColumn("status", upper(trim(col("status"))))
    )

# AutoLoader for Transaction Data
@dlt.table(
    comment="Raw transaction data from MySQL CDC stream",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def transaction_bronze_raw_full_cdc_history():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/transaction_schema")
        .option("mergeSchema", "true")
        .load(f"{source_path}/transactions")
        .withColumn("_file_name", input_file_name())
        .withColumn("_load_timestamp", current_timestamp())
    )

# Cleaned Transaction Bronze Data
@dlt.table(
    comment="Cleaned transaction bronze data with quality checks",
    table_properties={
        "quality": "bronze_clean",
        "pipelines.autoOptimize.managed": "true"
    }
)

@dlt.expect("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect("valid_account_reference", "account_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount != 0")
@dlt.expect("valid_transaction_type", "transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER')")
def transaction_bronze_clean_v():
    return (
        dlt.readStream("transaction_bronze_raw_full_cdc_history")
        .filter(col("op_type").isin("I", "U"))
        .withColumn("transaction_type", upper(trim(col("transaction_type"))))
        .withColumn("description", trim(col("description")))
        .withColumn("merchant", trim(col("merchant")))
        .withColumn("status", upper(trim(col("status"))))
    )