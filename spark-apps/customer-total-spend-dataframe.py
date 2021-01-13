from pyspark.sql import types, functions

from utils import SPARK_DATA_PATH, get_spark_session

spark = get_spark_session('Customer spend dataframe')

schema = types.StructType([
    types.StructField('customer_id', types.IntegerType(), True),
    types.StructField('order_id', types.IntegerType(), True),
    types.StructField('order_price', types.FloatType(), True),
])
df = spark.read.schema(schema).csv(
    f"{SPARK_DATA_PATH}/customer-orders.csv"
)

df.printSchema()

# Remove unused column
df = df.select('customer_id', 'order_price')

df = df.groupBy('customer_id').sum('order_price')

df = df.withColumn(
    'total',
    functions.round('sum(order_price)', 2)
).select('customer_id', 'total').sort('total')

df.show(df.count())
