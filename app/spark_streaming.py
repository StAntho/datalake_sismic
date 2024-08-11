from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    BooleanType,
    TimestampType,
)

# Creer la session Spark
spark = SparkSession.builder.appName("SismicStreaming").getOrCreate()

# Mute les logs inferieur au niveau Warning
spark.sparkContext.setLogLevel("WARN")

# Define the schema of the JSON messages
schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("secousse", BooleanType(), True),
        StructField("magnitude", FloatType(), True),
        StructField("tension_entre_plaque", FloatType(), True),
    ]
)

# Selectionner mon topic
kafka_topic_name = "topic1"

# Selectionner mon server
kafka_bootstrap_servers = 'kafka:9092'

# Recuperation de mes data de mon stream kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", kafka_topic_name) \
  .load()


# Parse the JSON messages
kafkaStream = df.selectExpr("CAST(value AS STRING) as json")
parsed = kafkaStream.select(from_json(col("json"), schema).alias("data")).select(
    "data.*"
)

# Convert timestamp to TimestampType
parsed = parsed.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Show the DataFrame
query = parsed.writeStream.outputMode("append").format("console").start()

# Save the DataFrame to HDFS
# query = (
#     parsed.writeStream.outputMode("append")
#     .format("parquet")
#     .option("path", "hdfs://namenode:9000/user/anthony/sismic_data_parquet")
#     .option("checkpointLocation", "hdfs://namenode:9000/user/anthony/sismic_data_checkpoint")
#     .start()
# )

query.awaitTermination()
