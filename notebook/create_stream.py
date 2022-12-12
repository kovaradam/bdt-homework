# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 
# MAGIC 
# MAGIC Establish connection to kafka servers and create select by schema

# COMMAND ----------

# import config variables
%run "./secrets"

# import schema
%run './schema'

# COMMAND ----------

import os

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# Subscribe to kafka topic
buses_stream_topic = spark.readStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers", aws_brokers) \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";') \
    .option("subscribe", "regbuses") \
    .load()

buses_select = buses_stream_topic.select(from_json(col("value").cast("string"), pid_schema).alias("data")).select("data.*") 

# COMMAND ----------


