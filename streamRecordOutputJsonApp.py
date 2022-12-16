"""
  Saves the record in the stream in a json file.

  @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging

logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s"
)


# The record structure must match the structure of your generated record
# (or your real record if you are not using generated records)
#{"call_time": "2022-12-16T07:32:52.055899", "duration": 2566, "call_phone": "9734317233", "caller": "Willa Howard"}
recordSchema = StructType([
    StructField('call_time', TimestampType(), True),
    StructField('duration', IntegerType(), True),
    StructField('call_phone', StringType(), True),
    StructField('caller', StringType(), True)
])


if __name__ == '__main__':
    logging.debug("-> start")

    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("Read lines over a file stream") \
        .master("local[*]")\
        .getOrCreate()


    # Reading the record is always the same
    df = spark.readStream\
        .format("json") \
        .schema(recordSchema) \
        .load("/tmp/streaming/in")


    # File output only supports append
    # Format is CSV
    # Output directory check point
    query = df.writeStream.outputMode("append") \
        .format("csv") \
        .option("path", "/tmp/spark/csv") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    query.awaitTermination()

    spark.stop()

    logging.debug("<- end")
