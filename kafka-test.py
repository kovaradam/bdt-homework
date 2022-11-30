import os
from dotenv import load_dotenv

load_dotenv()
username, password = os.environ["AWS_USERNAME"], os.environ["AWS_PASSWORD"]

JAAS = f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";'
tram_stream_topic = spark.readStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", JAAS) \
    .option("subscribe", "trams") \
    .load()
