# Databricks notebook source
# MAGIC %sql
# MAGIC select COUNT(*) as rows from datalake_amplitude_clean.events

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(rows) from (
# MAGIC   select
# MAGIC   year, month, day, COUNT(*) as rows
# MAGIC   from datalake_amplitude_clean.events
# MAGIC   group by 1,2,3
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datalake_amplitude_raw.events

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Shuffle Partitions
# MAGIC Because shuffles will be triggered by some workloads we need to manage the **`spark.sql.shuffle.partitions`**.
# MAGIC
# MAGIC The default number of shuffle partitions (200) can cripple many streaming jobs.
# MAGIC
# MAGIC As such, it's a reasonably good practice to simply use the maximum number of cores as the high end, and if smaller, maintain a factor of the number of course.
# MAGIC
# MAGIC Naturally, this generalized advice changes as you increase the number of streams running on a single cluster.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> Note that this value cannot be changed between runs without creating a new checkpoint for each stream.

# COMMAND ----------

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Optimize and Auto Compaction
# MAGIC
# MAGIC We'll want to ensure that our bronze table and 3 parsed silver tables don't contain too many small files. Turning on Auto Optimize and Auto Compaction help us to avoid this problem. For more information on these settings, <a href="https://docs.databricks.com/delta/optimizations/auto-optimize.html" target="_blank">consult our documentation</a>.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming with schema inference

# COMMAND ----------

def process_raw(source_path, table_name, checkpoint_directory, once=False, processing_time="1 hour"):
    from pyspark.sql import functions as F
        
    data_stream_writer = (spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", checkpoint_directory)
            .option("cloudFiles.includeExistingFiles", "true")
            .option("maxFilesPerTrigger", 1)
            .load(source_path)
            .withColumn("input_file_name", F.input_file_name())
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_directory)
            .option("mergeSchema", "true")
            .partitionBy("app", "event_type") 
            .queryName("streaming_raw_amplitude")
         )
    
    if once == True:
        return data_stream_writer.trigger(availableNow=True).table(table_name)
    else:
        return data_stream_writer.trigger(processingTime=processing_time).table(table_name)

# COMMAND ----------

source_path = "s3://5a-amplitude-events-external/170698/"
checkpoint_directory = "s3://5a-amplitude-events-external/170698-delta-checkpoint-directory/"

bronze_query = process_raw(
  source_path=source_path,
  table_name="amplitude_delta_temp",
  checkpoint_directory=checkpoint_directory,
  once=True
#   processing_time
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify if all files available were ingested

# COMMAND ----------

teste = dbutils.fs.ls("s3://5a-amplitude-events-external/170698/")

len(teste)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct input_file_name) from amplitude_delta_temp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming without schema inference

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
# from pyspark.sql.functions import input_file_name

# input_path = "s3://5a-amplitude-events-external/170698/"

# streaming_df_amplitude = (
#   spark
#   .readStream
#   .schema(spark.table("amplitude_delta_temp").schema)
#   .option("maxFilesPerTrigger", 1)
#   .json(input_path)
#   .withColumn("input_file_name", input_file_name()) 
# )

# display(streaming_df_amplitude)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY amplitude_delta_temp

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean automated

# COMMAND ----------

class Upsert:
  def __init__(self, sql_query, update_temp):
    self.sql_query = sql_query
    self.update_temp = update_temp 
        
  def upsert_to_delta(self, microBatchDF, batch):
      microBatchDF.createOrReplaceTempView(self.update_temp)
      microBatchDF._jdf.sparkSession().sql(self.sql_query)

def process_clean(app, event_type):
  deduped_df = (
    spark
    .readStream
    .table("amplitude_delta_temp")
    .filter(f"app = {app}")
    .filter(f"event_type = '{event_type}'")
    .dropDuplicates(["uuid"])
    .createOrReplaceTempView("streaming_tmp_vw")
  )
  
  spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW {app}_{event_type}_tmp_vw AS (
      SELECT
        amplitude_id AS id_amplitude,
        amplitude_attribution_ids AS ids_amplitude_attributed,
        user_id AS id_user,
        device_id AS id_device,
        event_id AS id_event,
        app AS id_app,
        uuid,
        adid,
        session_id AS id_session,
        '$schema' AS id_schema,
        '$insert_id' AS id_inserted,
        idfa,
        event_type,
        amplitude_event_type,
        city,
        country,
        data,
        device_brand,
        device_carrier,
        device_family,
        device_manufacturer,
        device_model,
        device_type,
        dma,
        ip_address,
        location_lat,
        location_lng,
        os_name,
        os_version,
        platform,
        library,
        region,
        start_version,
        language,
        version_name,
        sample_rate,
        event_properties,
        user_properties,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_source=([^&|$]+)', 1), ''), 'direct') AS up_utm_source,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_medium=([^&|$]+)', 1), ''), 'direct') AS up_utm_medium,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_campaign=([^&|$]+)', 1), ''), 'direct') AS up_utm_campaign,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_content=([^&|$]+)', 1), ''), 'direct') AS up_utm_content,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_term=([^&|$]+)', 1), ''), 'direct') AS up_utm_term,
        GET_JSON_OBJECT(event_properties, '$.house_id') AS ep_house_id,
        boolean(paying) AS is_paying,
        is_attribution_event,
        timestamp(server_received_time) AS ts_server_received,
        timestamp(event_time) AS ts_event,
        timestamp(server_upload_time) AS ts_server_uploaded,
        timestamp(user_creation_time) AS ts_user_created,
        timestamp(client_event_time) AS ts_client_event,
        timestamp(client_upload_time) AS ts_client_uploaded,
        timestamp(processed_time) AS ts_processed
    FROM
        streaming_tmp_vw
    )
    """.format(app=app, event_type=event_type)
  )
  
  spark.sql(
    """
    CREATE TABLE IF NOT EXISTS {app}_{event_type}_events_delta_temp
    USING DELTA
    LOCATION 's3://5a-amplitude-events-external/{app}_{event_type}_events_delta_temp'
    AS
      SELECT
        amplitude_id AS id_amplitude,
        amplitude_attribution_ids AS ids_amplitude_attributed,
        user_id AS id_user,
        device_id AS id_device,
        event_id AS id_event,
        app AS id_app,
        uuid,
        adid,
        session_id AS id_session,
        '$schema' AS id_schema,
        '$insert_id' AS id_inserted,
        idfa,
        event_type,
        amplitude_event_type,
        city,
        country,
        data,
        device_brand,
        device_carrier,
        device_family,
        device_manufacturer,
        device_model,
        device_type,
        dma,
        ip_address,
        location_lat,
        location_lng,
        os_name,
        os_version,
        platform,
        library,
        region,
        start_version,
        language,
        version_name,
        sample_rate,
        event_properties,
        user_properties,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_source=([^&|$]+)', 1), ''), 'direct') AS up_utm_source,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_medium=([^&|$]+)', 1), ''), 'direct') AS up_utm_medium,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_campaign=([^&|$]+)', 1), ''), 'direct') AS up_utm_campaign,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_content=([^&|$]+)', 1), ''), 'direct') AS up_utm_content,
        COALESCE(NULLIF(REGEXP_EXTRACT(GET_JSON_OBJECT(user_properties, '$.entrance_uri'), 'utm_term=([^&|$]+)', 1), ''), 'direct') AS up_utm_term,
        GET_JSON_OBJECT(event_properties, '$.house_id') AS ep_house_id,
        boolean(paying) AS is_paying,
        is_attribution_event,
        timestamp(server_received_time) AS ts_server_received,
        timestamp(event_time) AS ts_event,
        timestamp(server_upload_time) AS ts_server_uploaded,
        timestamp(user_creation_time) AS ts_user_created,
        timestamp(client_event_time) AS ts_client_event,
        timestamp(client_upload_time) AS ts_client_uploaded,
        timestamp(processed_time) AS ts_processed
    FROM
        amplitude_delta_temp
    WHERE 1=2
    """.format(app=app, event_type=event_type)
  )
  
  upsert_query = """
    MERGE INTO {app}_{event_type}_events_delta_temp a
    USING {app}_{event_type}_updates b
    ON a.uuid=b.uuid
    WHEN NOT MATCHED THEN INSERT *
  """.format(app=app, event_type=event_type)
  
  streaming_merge = Upsert(upsert_query, f"{app}_{event_type}_updates")
  
  query = (
    spark
    .table(f"{app}_{event_type}_tmp_vw")
    .writeStream
    .foreachBatch(streaming_merge.upsert_to_delta)
    .outputMode("update")
    .option(
      "checkpointLocation",
      f"s3://5a-amplitude-events-external/{app}_{event_type}_checkpoint"
    )
   .queryName(f"streaming_{app}_{event_type}")
   .trigger(availableNow=True)
   .start()
  )

  query.awaitTermination()

# COMMAND ----------

from bietlejuice.consumers.db_consumers.databricks_consumer import DatabricksConsumer
from bietlejuice.clients.db_clients import SparkClient
from pyspark.sql.functions import split


databricks_consumer = DatabricksConsumer(
    {"db": "default"}, SparkClient()
)
databricks_partition_values = databricks_consumer.get_partition_values_from_table(
    table_name="amplitude_delta_temp"
)

for row in databricks_partition_values.collect():
  app, event_type = row.asDict()['app'], row.asDict()['event_type']
  if app == "170698" and event_type == "home_page_viewed":
    process_clean(app, event_type)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct uuid) from amplitude_delta_temp
# MAGIC where app = 170698 and event_type = 'home_page_viewed'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from 170698_home_page_viewed_events_delta_temp

# COMMAND ----------

# MAGIC %md
# MAGIC # Próximos passos
# MAGIC - Comparar tabelas raw/clean auto loader com tabelas atuais
# MAGIC - Monitorar streaming
# MAGIC - Criar todas as tabelas clean

# COMMAND ----------


