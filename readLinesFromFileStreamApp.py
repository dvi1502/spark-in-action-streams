"""
  Reads a stream from a stream (files)

  @author rambabu.posa
"""
from pyspark.sql import SparkSession
import logging

logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s"
)


if __name__ == '__main__':
    logging.debug("-> start")

    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("Read lines from a file stream") \
        .master("local[*]")\
        .getOrCreate()


    df = spark\
        .readStream\
        .format("text")\
        .load("/tmp/streaming/in")

# Use below for Windows
# .load("C:/tmp/")

    # query = df\
    #     .writeStream\
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .option("numRows", 3) \
    #     .start()

    # File output only supports append
    # Format is JSON
    # Output directory
    # check point
    query = df.writeStream.outputMode("append") \
        .format("json") \
        .option("path", "/tmp/spark/json") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()


# # File output only supports append
# # Format is Apache Parquet: format = parquet
# # Output directory: path = /tmp/spark/parquet
# # check point: checkpointLocation = /tmp/checkpoint
#     query = df.writeStream.outputMode("append") \
#         .format("parquet")  \
#         .option("path", "/tmp/spark/parquet") \
#         .option("checkpointLocation", "/tmp/checkpoint") \
#         .start()

    query.awaitTermination()

    logging.debug("<- end")


