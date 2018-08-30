from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
from pyspark.ml.feature import CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vector as MLVector
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.ml.feature import CountVectorizer
import pandas as pd
#Start spark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SQLFinalTask") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# author|   subreddit|    id|         title|               time|score|num_comments|   domain
def getAuthorsStats(subredditDF):
    return subredditDF.groupBy(subredditDF.author)\
            .agg(
                count("*").alias("totalPosts"),
                avg(subredditDF.score).alias("avgScore"),
                sum(subredditDF.score).alias("totalScore"),
                max(subredditDF.score).alias("MaxScoreInAPost"),
                avg(subredditDF.num_comments).alias("avgComments"),
                sum(subredditDF.num_comments).alias("totalComments"),
                max(subredditDF.num_comments).alias("MaxCommentsInAPost"),
            )\
            .sort( desc("totalPosts"), desc("avgScore") )\
            .selectExpr("*")

def getSpikes(sDF):
    return sDF\
            .select( date_format('time','yyyy-MM-dd').alias('day'), sDF.score, sDF.num_comments  )\
            .groupBy( "day")\
            .agg( 
                count("*").alias("totalPostsInTheDay"),
                sum(sDF.score).alias("totalScore"),
                max(sDF.score).alias("maxScore"),
                avg(sDF.score).alias("avgScore"),
                sum(sDF.num_comments).alias("totalComments"),
                max(sDF.num_comments).alias("maxnComments"),
                avg(sDF.num_comments).alias("avgComments")
            )\
            .sort( desc("totalPostsInTheDay") )\
            .selectExpr("*")

def getSpikesText(sDF, spikesDF, amountOfSpikes=10):
    fixedSpikes = spikesDF.sort( desc("totalPostsInTheDay") ).limit(amountOfSpikes)
    return sDF.select( sDF.id, sDF.title, date_format('time','yyyy-MM-dd').alias('day') )\
              .join(fixedSpikes, "day" , "right")\
              .sort( desc("day") )\
              .select("day", "title", "id", "totalPostsInTheDay")

subreddits = ["The_Donald","politics"]
for subreddit in subreddits:
    df = spark.createDataFrame( pd.read_csv(f"{subreddit}/_{subreddit}.csv") ) 
    # getSpikes(df).repartition(1).write.csv(f'{subreddit}/spikes_{subreddit}.csv',header=True)        #Create file for picos
    # getAuthorsStats(df).repartition(1).write.csv(f'{subreddit}/authors_{subreddit}.csv',header=True) #Create file for authorsData
    # spikesDF = spark.read.csv(f'{subreddit}/spikes_{subreddit}.csv', header=True)
    # getSpikesText(df,spikesDF).repartition(1).write.csv(f'{subreddit}/spikesText_{subreddit}.csv',header=True)
    
