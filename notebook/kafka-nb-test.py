# Databricks notebook source
import os

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

%run "./secrets"



JAAS = f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";'
tram_stream_topic = spark.readStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", JAAS) \
    .option("subscribe", "trams") \
    .load()


schema_pid = StructType([
      StructField('geometry', StructType([
          StructField('coordinates', ArrayType(StringType()), True),
          StructField('type', StringType())])),
      StructField('properties', StructType([
          StructField('last_position', StructType([
              StructField('bearing', IntegerType()),
              StructField('delay', StructType([
                  StructField("actual", IntegerType()),
                  StructField("last_stop_arrival", StringType()),
                  StructField("last_stop_departure", StringType())])),
              StructField("is_canceled", BooleanType()),
              StructField('last_stop', StructType([
                  StructField("arrival_time", StringType()),
                  StructField("departure_time", StringType()),
                  StructField("id", StringType()),
                  StructField("sequence", IntegerType())])),
              StructField('next_stop', StructType([
                  StructField("arrival_time", StringType()),
                  StructField("departure_time", StringType()),
                  StructField("id", StringType()),
                  StructField("sequence", IntegerType())])),
              StructField("origin_timestamp", StringType()),
              StructField("shape_dist_traveled", StringType()),
              StructField("speed", StringType()),
              StructField("state_position", StringType()),
              StructField("tracking", BooleanType())])),
          StructField('trip', StructType([
              StructField('agency_name', StructType([
                  StructField("real", StringType()),
                  StructField("scheduled", StringType())])),
              StructField('cis', StructType([
                  StructField("line_id", StringType()),
                  StructField("trip_number", StringType())])),
              StructField('gtfs', StructType([
                  StructField("route_id", StringType()),
                  StructField("route_short_name", StringType()),
                  StructField("route_type", IntegerType()),
                  StructField("trip_headsign", StringType()),
                  StructField("trip_id", StringType()),
                  StructField("trip_short_name", StringType())])),
              StructField("origin_route_name", StringType()),
              StructField("sequence_id", IntegerType()),
              StructField("start_timestamp", StringType()),
              StructField("vehicle_registration_number", IntegerType()),
              StructField('vehicle_type', StructType([
                  StructField("description_cs", StringType()),
                  StructField("description_en", StringType()),
                  StructField("id", IntegerType())])),
              StructField("wheelchair_accessible", BooleanType()),
              StructField("air_conditioned", BooleanType())]))])),
      StructField("type", StringType())
  ])



base_trams = tram_stream_topic.select(from_json(col("value").cast("string"), schema_pid).alias("data")).select("data.*") 


tram_stream_mem_append = base_trams.writeStream \
        .format("memory")\
        .queryName("mem_trams")\
        .outputMode("append")\
        .start()






