from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Using SparkConf()

conf = SparkConf()
conf.setAppName('Test Master').setMaster('spark://localhost:7077')
sc = SparkContext(conf=conf)
rdd = sc.textFile('file:///opt/bitnami/spark/spark-data/Vermont_Vendor_Payments.csv')
print(rdd.count())

# Using SparkSession
spark = SparkSession \
    .builder \
    .appName('Test remote master') \
    .config("spark.master", "spark://localhost:7077") \
    .getOrCreate()

sc = spark.sparkContext
rdd = sc.textFile('file:///opt/bitnami/spark/spark-data/Vermont_Vendor_Payments.csv')
print(rdd.count())
