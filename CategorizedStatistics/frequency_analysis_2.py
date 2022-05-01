from pyspark.sql import SparkSession
from pyspark.sql.functions import concat

ss = SparkSession.builder.appName("frequency_analysis").getOrCreate()
ss.sparkContext.setLogLevel("WARN")

df = ss.read.csv("../YouTubeData/080327/3.txt", sep='\t', inferSchema=True)
videos_df = df.select(df._c0,df._c1,df._c2,df._c3,df._c4,df._c5,df._c6,df._c7,df._c8,
                concat(df._c9,df._c10,df._c11,df._c12,df._c13,df._c14,df._c15,df._c16,df._c17,df._c18,df._c19,df._c20).alias("Related Video IDs"))

users_df = ss.read.csv("../YouTubeData/080903user/user.txt", sep='\t', inferSchema=True)\
            .withColumnRenamed("_c0", "Username")\
            .withColumnRenamed("_c1", "# of Uploads")\
            .withColumnRenamed("_c2", "# of Friends")

videos_df.join(users_df, videos_df._c1 == users_df.Username, 'inner')\
    .sort(videos_df._c5, ascending=False)\
    .withColumnRenamed("_c0", "Video ID")\
    .withColumnRenamed("_c1", "Username")\
    .withColumnRenamed("_c2", "Age")\
    .withColumnRenamed("_c3", "Category")\
    .withColumnRenamed("_c4", "Length")\
    .withColumnRenamed("_c5", "Views")\
    .withColumnRenamed("_c6", "Rate")\
    .withColumnRenamed("_c7", "Ratings")\
    .withColumnRenamed("_c8", "Comments")\
    .show()
