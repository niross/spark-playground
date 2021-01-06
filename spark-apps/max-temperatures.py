from utils import get_spark_context, SPARK_DATA_PATH

# file definition:
# <weather station>, <date>, <temp type>, <temp (10ths of degrees celsius), *unused
# ITE00100554,1800101,TMAX,-75,,,E


def parse_line(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1
    return station_id, entry_type, temperature


sc = get_spark_context('RatingsHistogram')
lines = sc.textFile(f'{SPARK_DATA_PATH}/1800.csv')
parsed_lines = lines.map(parse_line)
# Filter for min templs only
max_temps = parsed_lines.filter(lambda x: "TMAX" in x[1])
# Remove tmin column from the data as it's not needed (now just station id and temp)
station_temps = max_temps.map(lambda x: (x[0], x[2]))
# Reduce down to the minimum temperature for every weather station id
max_temps = station_temps.reduceByKey(lambda x, y: max(x, y))
# Actually run it
results = max_temps.collect()

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))
