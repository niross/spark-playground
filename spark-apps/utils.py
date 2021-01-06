from pyspark import SparkConf, SparkContext

MASTER = 'spark://localhost:7077'
SPARK_DATA_PATH = 'file:///opt/bitnami/spark/spark-data'


def get_spark_context(name):
    conf = SparkConf().setMaster(MASTER).setAppName(name)
    return SparkContext(conf=conf)
