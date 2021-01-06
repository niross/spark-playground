# spark-submit spark-apps/friends-by-age.py
from utils import get_spark_context, SPARK_DATA_PATH

sc = get_spark_context('RatingsHistogram')


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


lines = sc.textFile(f'{SPARK_DATA_PATH}/fakefriends.csv')
rdd = lines.map(parse_line)

# rdd.mapValues(lambda x: (x, 1))
# -> Update the value for each row to be {age} => (num_friends, 1)
# and then
# .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) =>
# to get a total of all all friends for a specific age and the total number of times an age was seen
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# Then get an average by, for each value, dividing the total number of friends by the total times
# the age is seen
average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
# Actually run the actions
results = average_by_age.collect()
for result in results:
    print(result)
