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

print("Stats for magnitude:")
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

print("Stats by month:")
monthly_activity.orderBy("year", "month").show(15)
print("Stats by place:")
place_stats.orderBy("ville").show(15)
print("Stats by tension:")
tension.orderBy("tension entre plaque").show(15)

window_spec = Window.partitionBy("ville").orderBy("date")
df_ville = df_ville.withColumn("prev_time", lag("date").over(window_spec))
df_ville = df_ville.withColumn("time_seconds", unix_timestamp(col("date")))
df_ville = df_ville.withColumn("prev_time_seconds", unix_timestamp(col("prev_time")))
df_ville = df_ville.withColumn("time_diff", col("time_seconds") - col("prev_time_seconds"))

print("df_ville with new columns:")
df_ville.show(15)

small_quakes = df_ville.filter((col("magnitude") < 4.0) & (col("time_diff") < 3600 * 24 * 7))
print("Small quakes:")
small_quakes.show(15)

pre_event_window = Window.partitionBy("ville").orderBy("date").rangeBetween(0, 3600 * 24 * 7)

df_ville = df_ville.withColumn("big_quake_flag", when(col("magnitude") >= 5.0, col("magnitude")).otherwise(None))

df_ville = df_ville.withColumn("next_big_quake", max(col("big_quake_flag")).over(window_spec))
print("Big quakes:")
df_ville.select("ville", "date", "magnitude", "next_big_quake").show(10, truncate=False)

print("Stats for a day:")
daily_df = df.groupBy(date_format(col("date"), "yyyy-MM-dd").alias("day")) \
             .agg(count("*").alias("num_events"),
                  avg("magnitude").alias("avg_magnitude"),
                  min("magnitude").alias("min_magnitude"),
                  max("magnitude").alias("max_magnitude"))

daily_df.show(10, truncate=False)

print("Stats for 1hour:")
hourly_df = df.groupBy(date_format(col("date"), "yyyy-MM-dd HH:00:00").alias("hour")) \
              .agg(count("*").alias("num_events"),
                   avg("magnitude").alias("avg_magnitude"),
                   min("magnitude").alias("min_magnitude"),
                   max("magnitude").alias("max_magnitude"))

hourly_df.show(10, truncate=False)

