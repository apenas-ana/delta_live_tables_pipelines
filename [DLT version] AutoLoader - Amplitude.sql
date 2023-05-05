-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE amplitude_bronze_dlt
(
  CONSTRAINT valid_uuid EXPECT (uuid IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (app, event_type)
COMMENT "Raw data of QuintoAndar's website/app visitors tracking"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  input_file_name() AS input_file_name
FROM
  cloud_files("s3://5a-amplitude-events-external/170698/", "json");

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE amplitude_silver_dlt
PARTITIONED BY (id_app, event_type)
COMMENT "Clean data of QuintoAndar's website/app visitors tracking"
TBLPROPERTIES ("quality" = "silver")
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
  STREAM(LIVE.amplitude_bronze_dlt)

-- COMMAND ----------


