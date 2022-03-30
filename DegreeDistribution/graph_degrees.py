from pyspark import SparkContext
import re

sc = SparkContext("local", "graph_degrees")

out_degree = sc.textFile("../YouTubeData/080327/1.txt")\
		.map(lambda line: re.split(r'\t+', line.rstrip('\t')))\
		.map(lambda line: (line[0], len(line[9:])))\
		.saveAsTextFile('out_degree')

in_degree = sc.textFile("../YouTubeData/080327/1.txt")\
		.map(lambda line: re.split(r'\t+', line.rstrip('\t')))\
		.flatMap(lambda line: [(node, 1) for node in line[9:]])\
		.reduceByKey(lambda curr, value: curr + value )\
		.saveAsTextFile('in_degree')

