# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook serves to run periodically and collect data to table

# COMMAND ----------

# MAGIC %run "./create_stream"

# COMMAND ----------

buses_select.writeStream \
        .queryName("buses_hourly")\
        .trigger(once=True)\
        .option('checkpointLocation','./hourly_checkpoint')\
        .format("delta").outputMode("append").toTable("buses_hourly")

# COMMAND ----------


