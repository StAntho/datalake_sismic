from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("explore_csv_from_hdfs").getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/user/anthony/sismic_data/dataset_sismique.csv", header=True, inferSchema=True)
df_ville = spark.read.csv("hdfs://namenode:9000/user/anthony/sismic_data/dataset_sismique_villes.csv", header=True, inferSchema=True)

# Suppression des doublons
df = df.dropDuplicates()
df_ville = df_ville.dropDuplicates()

df.printSchema()
df_ville.printSchema()
df.show(15)
df_ville.show(15)