# Databricks notebook source
import dlt
from pyspark.sql.functions import input_file_name

def generate_table(live_table):
  @dlt.table(
    name=live_table,
    comment=f"Olos Dialer raw data for {live_table}"
  )
  def create_live_table():
    return (
      spark
      .readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"s3://5a-discador-olos/db/QuintoAndar/ExportFiles/S3/{live_table}/")
      .withColumn("input_file_name", input_file_name())
    )

tables_list = [
  'AgentSupervisor',
  'AgentStateRawData',
  'Campaign',
  'CampaignCustomer',
  'ConfigReasons',
  'Customer',
  'Disposition',
  'DispositionDetail',
  'DispositionPlan',
  # 'InboundRawData',
  'Info_AgentStatus',
  'Info_CallDirection',
  'Info_CampaignType',
  'Info_DispositionType',
  'Info_PbxDisposition',
  'Info_ReasonType',
  'Info_StatusId',
  'LoginRawData',
  'MailingInformation',
  'AttemptsRawData',
  'Reason',
  'Users',
  'PbxBillingData'
]

[generate_table(t) for t in tables_list]
