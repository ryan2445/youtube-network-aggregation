from pyspark import SparkContext
from pyspark.sql import SparkSession
import re

sc = SparkContext("local", "graph_degrees")
ss = SparkSession.builder.appName("frequency_analysis").getOrCreate()
ss.sparkContext.setLogLevel("WARN")

text_file = sc.textFile("../YouTubeData/080327/3.txt")

out_degree = text_file.map(lambda line: re.split(r'\t+', line.rstrip('\t')))\
				.map(lambda line: (line[0], len(line[9:])))\
				.sortBy(lambda video: video[1])

out_degree_values = out_degree.map(lambda line: line[1])
out_degree_avg = round(out_degree_values.mean())
out_degree_max = out_degree_values.max()
out_degree_min = out_degree_values.min()
sc.parallelize([{'out_degree_avg': out_degree_avg,\
					'out_degree_max': out_degree_max,\
					'out_degree_min': out_degree_min}])\
					.saveAsTextFile('out_degree_analysis')

in_degree = text_file.map(lambda line: re.split(r'\t+', line.rstrip('\t')))\
				.flatMap(lambda line: [(node, 1) for node in line[9:]])\
				.reduceByKey(lambda curr, value: curr + value )\
				.sortBy(lambda video: video[1])

in_degree_values = in_degree.map(lambda line: line[1])
in_degree_avg = round(in_degree_values.mean())
in_degree_max = in_degree_values.max()
in_degree_min = in_degree_values.min()
sc.parallelize([{'in_degree_avg': in_degree_avg,\
					'in_degree_max': in_degree_max,\
					'in_degree_min': in_degree_min}])\
					.saveAsTextFile('in_degree_analysis')

largestOutDegreeVideos = out_degree.collect()
largestOutDegreeVideos = largestOutDegreeVideos[len(largestOutDegreeVideos) - 5:]

largestInDegreeVideos = in_degree.collect()
largestInDegreeVideos = largestInDegreeVideos[len(largestInDegreeVideos) - 5:]

out_degree.saveAsTextFile('out_degree')
in_degree.saveAsTextFile('in_degree')

df = ss.read.csv("../YouTubeData/080327/3.txt", sep='\t')
df[df._c0.isin(list(map(lambda video: video[0], largestOutDegreeVideos)))].show()
