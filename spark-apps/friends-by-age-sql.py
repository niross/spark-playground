from utils import SPARK_DATA_PATH, get_spark_session

spark = get_spark_session('Spark SQL')

data = spark.read.option('header', 'true').option('inferSchema', 'true').csv(
    f'{SPARK_DATA_PATH}/fakefriends-header.csv'
)

data.printSchema()

data = data.select('age', 'friends')

data.groupBy('age').avg('friends').sort('age').show()

