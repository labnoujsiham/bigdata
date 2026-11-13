from pyspark.sql import SparkSession

URI = "mongodb://192.168.141.32:27017,192.168.141.60:27017,192.168.141.171:27017/?replicaSet=rs0"

spark = (
    SparkSession.builder.appName("connector-test")
    .config("spark.mongodb.read.connection.uri", URI)
    .config("spark.mongodb.write.connection.uri", URI)
    .getOrCreate()
)

df = spark.createDataFrame([("v1", "Truck"), ("v2", "Van")], ["vehicle_id", "type"])

df.write.format("mongodb").mode("append").option("database", "fleet").option("collection", "spark_test").save()

back = spark.read.format("mongodb").option("database", "fleet").option("collection", "spark_test").load()
back.show()

spark.stop()