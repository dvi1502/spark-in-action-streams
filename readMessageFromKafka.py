
import logging
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               TimestampType, DoubleType, BooleanType)
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

checkpoint_path = "/tmp/spark/checkpoint"
kafka_servers = "172.22.0.1:9091,172.22.0.1:9092,172.22.0.1:9093"
topic = "input-calls,test"

logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s"
)

schemaValue = StructType([
     StructField('call_time', TimestampType(), False),
     StructField('duration', IntegerType(), False),
     StructField('call_phone', StringType(), False),
     StructField('caller', StringType(), False),
])

def directory_clear(target):
    print("deltree", target)
    for d in os.listdir(target):
        try:
            directory_clear(target + '/' + d)
        except OSError:
            os.remove(target + '/' + d)

    os.rmdir(target)

try:
    directory_clear(checkpoint_path)
except Exception as e:
    print("Exception: ", e)


if __name__ == '__main__':
    logging.debug("-> start")

    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("Read lines from a file stream") \
        .master("local[*]")\
        .getOrCreate()


    callDF = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", kafka_servers)\
        .option("subscribe", topic)\
        .option("startingOffsets", "earliest")\
        .load()

    callDF.printSchema()
    # root
    #  |-- key: binary (nullable = true)
    #  |-- value: binary (nullable = true)
    #  |-- topic: string (nullable = true)
    #  |-- partition: integer (nullable = true)
    #  |-- offset: long (nullable = true)
    #  |-- timestamp: timestamp (nullable = true)
    #  |-- timestampType: integer (nullable = true)

    callDF = callDF\
        .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value ", "timestamp", "topic")\
        .select(F.regexp_replace('value', r'(\\")', '"').alias("value"), F.col("key"), F.col("topic"))\
        .select(F.regexp_replace('value', r'("\{)', '{').alias("value"), F.col("key"), F.col("topic"))\
        .select(F.regexp_replace('value', r'(\}")', '}').alias("value"), F.col("key"), F.col("topic"))\
        .select(F.from_json(F.col("value"), schemaValue).alias("t"),F.col("value"))\
        .select("t.call_time","t.duration","t.call_phone","t.caller")

    callDF = callDF\
        .writeStream.format("console")\
        .option("truncate","false")\
        .outputMode("append")\
        .option("checkpointLocation",checkpoint_path)\
        .trigger(processingTime="15 seconds")\
        .start()\
        .awaitTermination(timeout=60)

    # query = callDF\
    #     .writeStream\
    #     .format("csv")\
    #     .option("path","/tmp/spark/kafka")\
    #     .option("checkpointLocation",checkpoint_path)\
    #     .outputMode("append")\
    #     .trigger(processingTime="15 seconds")\
    #     .start()\
    #     .awaitTermination(timeout=180)

    spark.stop()
    logging.debug("<- end")

