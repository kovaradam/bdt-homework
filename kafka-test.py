import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
username, password, broker_servers = os.environ["AWS_USERNAME"], os.environ[
    "AWS_PASSWORD"], os.environ["AWS_BROKER_SERVERS"]

spark = SparkSession.builder.getOrCreate()


JAAS = f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";'
tram_stream_topic = spark.readStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers", broker_servers) \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", JAAS) \
    .option("subscribe", "trams") \
    .load()
