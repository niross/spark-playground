# spark-submit spark-apps/ratings-counter.py
import collections
from utils import get_spark_context, SPARK_DATA_PATH


sc = get_spark_context('RatingsHistogram')

lines = sc.textFile(f'{SPARK_DATA_PATH}/ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
