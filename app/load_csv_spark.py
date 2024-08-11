from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV to SQLite") \
    .config("spark.driver.extraClassPath", "/path/to/sqlite-jdbc.jar") \
    .getOrCreate()

csv_file = "/app/data/dataset_sismique.csv"
df = spark.read.csv(csv_file, header=True, inferSchema=True)

sqlite_db_path = "/app/superset_home/your_database.db"
df.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:{}".format(sqlite_db_path)) \
    .option("dbtable", "your_table_name") \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()

spark.stop()