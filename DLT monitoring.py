# Databricks notebook source
# MAGIC %sql
# MAGIC select * from dlt_olos_dialer_tests.users

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE dlt_tests.amplitude_bronze_dlt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`s3://5a-amplitude-events-external/dlt_storage_location/system/events`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC id_app,
# MAGIC count(distinct uuid)
# MAGIC from
# MAGIC datalake_amplitude_clean.events
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table dlt_tests.amplitude_pipeline_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dlt_tests.amplitude_pipeline_logs
# MAGIC AS SELECT * FROM delta.`s3://5a-amplitude-events-external/dlt_storage_location/system/events`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`s3://5a-amplitude-events-external/dlt_storage_location/system/events`
# MAGIC where origin.update_id = '4ce763c3-5d60-44da-b62a-e2beb9d94d96'
# MAGIC and event_type = 'update_progress'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC origin.flow_name,
# MAGIC date(timestamp) as date,
# MAGIC SUM(details:flow_progress.metrics.num_output_rows) AS num_output_rows,
# MAGIC SUM(details:flow_progress.data_quality.dropped_records) AS dropped_records
# MAGIC from delta.`s3://5a-amplitude-events-external/dlt_storage_location/system/events`
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC AND details:flow_progress.metrics.num_output_rows IS NOT NULL
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# Creates the input box at the top of the notebook.
dbutils.widgets.text('storage', 'dbfs:/pipelines/production-data', 'Storage Location')

# COMMAND ----------

# Replace the text in the Storage Location input box to the desired pipeline storage location. This can be found in the pipeline configuration under 'storage'.
storage_location = dbutils.widgets.get('storage')
event_log_path = storage_location + "/system/events"
 
# Read the event log into a temporary view so it's easier to query.
event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM event_log_raw LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC # user events auditing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name FROM event_log_raw WHERE event_type = 'user_action'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM event_log_raw WHERE event_type = 'flow_progress'

# COMMAND ----------

# MAGIC %md
# MAGIC # Get the ID of the most recent pipeline update

# COMMAND ----------

# Save the most recent update ID as a Spark configuration setting so it can used in queries.
latest_update_id = spark.sql("SELECT origin.update_id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1").collect()[0].update_id
spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # lineage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets FROM event_log_raw WHERE event_type = 'flow_definition' AND origin.update_id = '${latest_update.id}'

# COMMAND ----------

# MAGIC %md
# MAGIC # data quality

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       event_log_raw
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress'
# MAGIC       AND origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name

# COMMAND ----------


