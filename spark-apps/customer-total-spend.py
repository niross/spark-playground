import re

from utils import get_spark_context, SPARK_DATA_PATH


def parse_line(text):
    split_line = text.split(',')
    return int(split_line[0]), float(split_line[-1])


sc = get_spark_context('CustomerTotalSpend')
inpt = sc.textFile(f"{SPARK_DATA_PATH}/customer-orders.csv")
orders = inpt.map(parse_line)

orders = orders.map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1])

results = orders.collect()

for result in results:
    print(str(result[0]) + ':\t\t' + str(result[1]))
