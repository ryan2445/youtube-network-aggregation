from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("frequency_analysis").getOrCreate()
ss.sparkContext.setLogLevel("WARN")

# ID | Length | Size | Bitrate
df = ss.read.csv("../YouTubeData/080908sizerate/sizerate.txt", sep='\t', inferSchema=True, header=True)

print("Length < 100:", df.where(df.Length < 100).count())
print("Length > 100 & < 400:", df.where((df.Length > 100) & (df.Length < 400)).count())
print("Length > 400 & < 600:", df.where((df.Length > 400) & (df.Length < 600)).count())
print("Length > 600:", df.where(df.Length > 600).count())

df.show(5, False)
