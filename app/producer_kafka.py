from kafka import KafkaProducer
from pyspark.sql import SparkSession
import json
import time

spark = SparkSession.builder \
	.appName("Read file") \
	.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

file_path = '/app/data/dataset_sismique.csv'
data = spark.read.csv(file_path, header=True)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'topic1'

# try:
while True:
    for row in data.collect():
        message = {
            "timestamp": row['date'],
            "secousse": row['secousse'],
            "magnitude": row['magnitude'],
            "tension_entre_plaque": row['tension entre plaque']
        }
        producer.send(topic_name, value=message)
        print("Sent: {}".format(message))
        time.sleep(1)
# except KeyboardInterrupt:
#     print("Stopping producer...")

producer.close()
