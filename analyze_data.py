mhash = hash
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
from datetime import datetime
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

def getSpikesText(sDF, spikesDF, amountOfSpikes=15):
    fixedSpikes = spikesDF.sort( desc("totalPostsInTheDay") ).limit(amountOfSpikes)
    return sDF.select( sDF.id, sDF.subreddit, sDF.title, date_format('time','yyyy-MM-dd').alias('day') )\
              .join(fixedSpikes, "day" , "right")\
              .sort( desc("day") )\
              .select(  "day", 
                        translate("title", ",", " ").alias("title"),
                        "id", 
                        "totalPostsInTheDay", 
                        "subreddit")

from requests import get
from time import sleep
headers = {'User-Agent': 'Mozilla/5.0'}
def getPostBody(row):
    #sort=controversial
    url = f"http://reddit.com/r/{row.subreddit}/comments/{row.id}.json?sort=confidence"
    _comments = []
    tries = 5
    while tries>0:
        r = get(url, headers=headers)
        if r.status_code == 200:
            responseJson  = r.json()

            post = responseJson[0]["data"]["children"][0]["data"]
            comments = responseJson[1]["data"]["children"]

            for comment in comments:
                comment = comment["data"]       

                _comments.append([
                    comment["id"],
                    comment["score"].
                    comment["ups"],
                    comment["body"],
                    comment["author"],
                    datetime.utcfromtimestamp( comment["created_utc"] ).isoformat(' ')
                ])
        else:
            sleep(1)
            print("ERROR")
        tries-=1
    return tuple( r for r in row) + tuple(_comments)


def getKeywordsInDataRange(sDF,oldestTime,newestTime,topics=1,wordsPerTopic=20):  #yyyy-MM-dd
    #Filter
    oldestTime = datetime.strptime(oldestTime, '%Y-%m-%d')
    newestTime = datetime.strptime(newestTime, '%Y-%m-%d')
    filteredText = sDF\
                    .select( "id", date_format('day','yyyy-MM-dd').alias('time'), col("title").alias("text") )\
                    .where( (col("time") >= oldestTime) & (col("time") <= newestTime) ) 
    
    #StartPipeline for preparing data
    textToWords = RegexTokenizer(inputCol="text", outputCol="splitted", pattern="[\\P{L}]+" ) #Remove signs and split by spaces
    stopRemover = StopWordsRemover(inputCol="splitted", outputCol="words", stopWords=StopWordsRemover.loadDefaultStopWords("english"))
    countVectorizer = CountVectorizer(inputCol="words", outputCol="features")
    pipeline = Pipeline(stages=[textToWords, stopRemover, countVectorizer])

    #GetCorups for LDA
    model  = pipeline.fit(filteredText)
    result = model.transform(filteredText)
    corpus = result.select("id", "features").rdd.map(lambda r: [mhash(r.id)%10**8,Vectors.fromML(r.features)]).cache()

    # Cluster the documents into k topics using LDA
    ldaModel = LDA.train(corpus, k=topics,maxIterations=100,optimizer='online')
    topics = ldaModel.topicsMatrix()
    vocabArray = model.stages[2].vocabulary #CountVectorizer
    topicIndices = spark.sparkContext.parallelize(ldaModel.describeTopics(maxTermsPerTopic = wordsPerTopic))

    def topic_render(topic):  # specify vector id of words to actual words
        terms = topic[0]
        result = []
        for i in range(wordsPerTopic):
            term = vocabArray[terms[i]]
            result.append(term)
        return result

    # topics_final = topicIndices.map(lambda topic: topic_render(topic)).collect()
    # for topic in range(len(topics_final)):
    #     print ("Topic" + str(topic) + ":")
    #     for term in topics_final[topic]:
    #         print (term)
    #     print ('\n')
    return topicIndices.map(lambda topic: topic_render(topic)).collect()

subreddits = ["The_Donald","politics"]
detectedSpikes = {
    "The_Donald":[
        ("2018-08-23","2018-08-25"),
        ("2018-07-28","2018-07-31"),
        ("2018-06-23","2018-08-27"),
    ]
}

for subreddit in subreddits:
    subredditDetectedSpikes = detectedSpikes[subreddit] if subreddit in detectedSpikes else []
    #Read Data
    df = spark.createDataFrame( pd.read_csv(f"{subreddit}/_{subreddit}.csv") ) 
    # spikesDF = spark.read.csv(f'{subreddit}/spikes_{subreddit}.csv', header=True)
    spikesTextDF = spark.read.csv(f'{subreddit}/spikesText_{subreddit}.csv', header=True)
    # authorsDF = spark.read.csv(f'{subreddit}/authors_{subreddit}.csv', header=True)
    #Generate
    # spikesDF = getSpikes(df)
    # authorsDF = getAuthorsStats(df)
    # spikesTextDF = getSpikesText(df,spikesDF)
    #Save
    # spikesDF.repartition(1).write.csv(f'{subreddit}/spikes_{subreddit}.csv',header=True)        #Create file for picos
    # authorsDF.repartition(1).write.csv(f'{subreddit}/authors_{subreddit}.csv',header=True) #Create file for authorsData
    # spikesTextDF.repartition(1).write.csv(f'{subreddit}/spikesText_{subreddit}.csv',header=True)
    # For each row in spikesTextDF: row_with_comments = getPostBody(row)
    # spikesTextDF.show()
    
    #Las fechas salen de la tabla spikes, esta ordenada por posts en ese dia 
    for sp in subredditDetectedSpikes:
        print(f"SPIKE {sp[0]} : {sp[1]}")
        #Automatizar
        topics = getKeywordsInDataRange( spikesTextDF ,sp[0] ,sp[1], topics=3,wordsPerTopic=20)
        for i,topic in enumerate(topics):
            print(f"\tTopic {i}")
            for keyword in topic:
                print(f"\t\t{keyword}")
    
