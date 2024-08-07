from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("explore_csv_from_hdfs").getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/user/anthony/sismic_data/dataset_sismique.csv", header=True, inferSchema=True)
df_ville = spark.read.csv("hdfs://namenode:9000/user/anthony/sismic_data/dataset_sismique_villes.csv", header=True, inferSchema=True)

# Suppression des doublons
df = df.dropDuplicates()
df_ville = df_ville.dropDuplicates()

df = df.withColumn("year", year(col("date")))
df = df.withColumn("month", month(col("date")))

df.printSchema()
df_ville.printSchema()
df.show(15)
df_ville.show(15)

df.describe('magnitude').show()
monthly_activity = df.groupBy("year", "month").agg(
    count("*").alias("event_count"),
    avg("magnitude").alias("average_magnitude"),
    max("magnitude").alias("max_magnitude")
)
place_stats = df_ville.groupBy("ville").agg(
    count("magnitude").alias("count"),
    mean("magnitude").alias("mean_magnitude"),
    stddev("magnitude").alias("stddev_magnitude"),
    min("magnitude").alias("min_magnitude"),
    max("magnitude").alias("max_magnitude")
)
tension = df.groupBy("tension entre plaque").agg(
    count("magnitude").alias("count"),
    mean("magnitude").alias("mean_magnitude"),
    stddev("magnitude").alias("stddev_magnitude"),
    min("magnitude").alias("min_magnitude"),
    max("magnitude").alias("max_magnitude")
)

correlation_tension_mag = df.stat.corr("tension entre plaque", "magnitude")
print("Correlation between tension and magnitude: {}".format(correlation_tension_mag))

monthly_activity.orderBy("year", "month").show(15)
place_stats.orderBy("ville").show(15)
tension.orderBy("tension entre plaque").show(15)

window_spec = Window.partitionBy("ville").orderBy("date")
df_ville = df_ville.withColumn("prev_time", lag("date").over(window_spec))
df_ville = df_ville.withColumn("time_seconds", unix_timestamp(col("date")))
df_ville = df_ville.withColumn("prev_time_seconds", unix_timestamp(col("prev_time")))
df_ville = df_ville.withColumn("time_diff", col("time_seconds") - col("prev_time_seconds"))

df_ville.show(15)

small_quakes = df_ville.filter((col("mag") < 4.0) & (col("time_diff") < 3600 * 24 * 7))
small_quakes.show(15)

pre_event_window = Window.partitionBy("ville").orderBy("date").rangeBetween(0, 3600 * 24 * 7)

df_ville = df_ville.withColumn("next_big_quake", expr("""
    max(case when mag >= 5.0 then mag else null end) over (pre_event_window)
"""))

pre_big_quakes = df_ville.filter(col("next_big_quake").isNotNull())
pre_big_quakes.select("region", "time", "mag", "next_big_quake").show(10)