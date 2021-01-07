import re

from utils import get_spark_context, SPARK_DATA_PATH


def normalise(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


sc = get_spark_context('RatingsHistogram')
inpt = sc.textFile(f"{SPARK_DATA_PATH}/Book")
words = inpt.flatMap(normalise)

# Convert each word to a key value pair with the 2nd value being one.
# Then reduce them down two single keys while adding up the times the word was encountered
word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Flip the value and key so the key is now the word count and value is the word
word_counts_sorted = word_counts.map(lambda x: (x[1], x[0])).sortByKey()

results = word_counts_sorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(str(word) + ':\t\t' + count)
