# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from faker import Faker
import time

fake = Faker()

spark = SparkSession.builder \
    .appName("FakeTweetsStreaming") \
    .master("local[*]") \
    .getOrCreate()

streaming_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 8888) \
    .load()

query = streaming_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

def generate_fake_tweets():
    while True:
        tweet = fake.text()
        print(f"Generated Tweet: {tweet}")
        time.sleep(2)  

import threading
thread = threading.Thread(target=generate_fake_tweets)
thread.start()

query.awaitTermination()
