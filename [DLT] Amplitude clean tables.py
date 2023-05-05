# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

def generate_table(arg):
#   @dlt.expect("valid_current_page_title", "current_page_title IS NOT NULL")
#   @dlt.expect_or_fail("valid_count", "click_count > 0")

  app, event_type = arg[0], arg[1]
  live_table = f"{app}_{event_type}_silver_dlt"
  @dlt.table(
    name= live_table,
    comment="Clean data for " + live_table
  )
  def create_live_table():
    return (
      dlt
      .read_stream("amplitude_silver_dlt")
      .filter(f"id_app = {app}")
      .filter(f"event_type = '{event_type}'")
    )

partitions_list = [
  (170698, "listing_page_viewed"),
  (170698, "home_page_viewed")
]

[generate_table(c) for c in partitions_list]

# spark.sparkContext.parallelize(partitions_list).map(generate_table)

# COMMAND ----------

# from bietlejuice.consumers.db_consumers.databricks_consumer import DatabricksConsumer
# from bietlejuice.clients.db_clients import SparkClient
# from pyspark.sql.functions import split

# databricks_consumer = DatabricksConsumer(
#     {"db": "dlt_tests"}, SparkClient()
# )
# databricks_partition_values = databricks_consumer.get_partition_values_from_table(
#     table_name="amplitude_bronze_dlt"
# )

# COMMAND ----------


