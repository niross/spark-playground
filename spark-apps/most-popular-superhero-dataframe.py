from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from utils import SPARK_DATA_PATH, get_spark_session

spark = get_spark_session('MostPopularSuperhero')

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)]
)

names = spark.read.schema(schema).option("sep", " ").csv(f"{SPARK_DATA_PATH}/Marvel-names.txt")

lines = spark.read.text(f"{SPARK_DATA_PATH}/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn(
    "id", func.split(
        func.trim(func.col("value")), " "
    )[0]
).withColumn(
    "connections",
    func.size(
        func.split(
            func.trim(
                func.col("value")
            ), " "
        )
    ) - 1
).groupBy("id").agg(
    func.sum("connections").alias("connections")
)
    
mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

