from pyspark import SparkContext
import re

sc = SparkContext("local", "graph_degrees")

graph = sc.textFile("../YouTubeData/080327/0.txt")\
		.map(lambda line: re.split(r'\t+', line.rstrip('\t')))\
		.map(lambda line: (line[0], line[9:]))\
		.collect()

_in = {}
out = {}
for i in range(len(graph)):
	curr = graph[i]
	node = curr[0]
	vertices = curr[1]
	out[node] = len(vertices)
	for j in range(len(vertices)):
		if _in.get(vertices[j]):
			_in[vertices[j]] += 1
		else:
			_in[vertices[j]] = 1

combined_in_out = {node: {'in': _in.get(node) or 0, 'out': out.get(node) or 0} for node in out.keys()}

rdd = sc.parallelize([combined_in_out])

rdd.saveAsTextFile("output")