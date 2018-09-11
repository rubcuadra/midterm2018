from keywordScrapper import run as scrapper
from keywordScrapper import types
from charts import run as chart

if __name__ == '__main__':
    dumpType = types.CSV
    subreddits = ["politics","news","The_Donald","truenews","PoliticalHumor","democrats","all"] #"ElectionPolls", "Ask_Politics"
    keyWords   = ["latino", "hispanic"] #"misinformation", "midterm" 
    oldest_post_date = "10/09/2017"  #dd/mm/yyyy
    
    #Create CSV files with posts
    # scrapper(subreddits,keyWords,oldest_post_date,dumpType)
    #Create .png files from the CSV files
    # chart(subreddits)
    