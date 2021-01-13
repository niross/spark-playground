from pyspark.sql import functions as func
from utils import SPARK_DATA_PATH, get_spark_session

spark = get_spark_session('Word count')

# Read each line of my book into a dataframe
inputDF = spark.read.text(f"{SPARK_DATA_PATH}/Book")

# Split using a regular expression that extracts words
words = inputDF.select(
    # Explode out into a new row for every word in the book
    func.explode(
        # Split at word boundaries
        func.split(inputDF.value, "\\W+")
    ).alias("word")  # Name the new column "word"
)
words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())
