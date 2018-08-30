import csv
from itertools import product
from requests import get
from time import sleep
from json import dump
from os import path, makedirs, cpu_count
from datetime import datetime
from multiprocessing import Pool
from enum import Enum
import pandas as pd

class types(Enum):
    JSON = "J"
    CSV  = "C"

#Inputs
dumpType = types.CSV
subreddits = ["ElectionPolls","politics","news","The_Donald","truenews","PoliticalHumor","shittypolitics","shittynews","Voting","democrats","Ask_Politics","all"]
keyWords   = ["latino", "hispanic", "misinformation", "midterm"] 
oldest_post_date = "28/08/2017"  #dd/mm/yyyy

def isPostValid(post):
    #example: if "something" in post["title"] ...
    #We can filter by keywords, otherwise we dump all posts
    #pprint(post)
    return True

def dumpSubredditPosts(payload):
    subreddit, keyword = payload[0],payload[1] #Autogenerated Tuple
    keyword = keyword.replace(" ","%20")
    headers = {'User-Agent': 'Mozilla/5.0'}
    limit = 25       #Number of posts per page
    page = 1         #for offsets
    
    oldest_pd = datetime.strptime(oldest_post_date, '%d/%m/%Y')
    last_post_date = datetime.today() #Just to start
    
    url = f"https://www.reddit.com/r/{subreddit}/search.json?q={keyword}&sort=new&limit={limit}&count={limit}&restrict_sr=1"
    # print (url)
    # COMMENTS: https://www.reddit.com/r/{subreddit}/comments/{postId}/new.json
    
    dumpFolder = f'{subreddit}'
    try:    
        if not path.exists(dumpFolder): makedirs(dumpFolder) #Create dir if required
    except: pass
    #Start
    after = None
    while oldest_pd<last_post_date: 
        customUrl = url if after is None else f"{url}&after={after}"
        r = get(customUrl, headers=headers) #i is the pager
        if r.status_code == 200:
            data  = r.json()["data"]
            posts = data["children"]
            after = data["after"]
            for post in posts:
                # after = post["data"]["id"]
                if isPostValid(post["data"]): 
                    if dumpType == types.JSON:
                        fp = dict(**post["data"], kind=post["kind"])                     #fixed Post
                        with open(f'{dumpFolder}/{fp["id"]}.json', 'w+') as o:dump(fp, o) #output file
                    elif dumpType == types.CSV:
                        timeInIso = datetime.utcfromtimestamp( post["data"]["created_utc"] ).isoformat(' ')
                        post_line = [post["data"]["author"],post["data"]["subreddit_name_prefixed"],post["data"]["id"],post["data"]["title"], timeInIso, post["data"]["score"], post["data"]["num_comments"], post["data"]["domain"]]
                        with open(f"{dumpFolder}/{subreddit}.csv", 'a') as f:
                            csv.writer(f).writerow(post_line)
                    last_post_date = datetime.utcfromtimestamp( post["data"]["created_utc"] )  #Update date
                    if oldest_pd>last_post_date: 
                        break
            print(after)
            print(subreddit,last_post_date)
            if after is None: break #No more to crawl
        else:
            # print(r.json()["message"]) #Max requests - wait
            sleep(1)
    print(subreddit,"DONE")

if __name__ == '__main__':
    pool = Pool(cpu_count()) #Parallelize crawlers
    pool.map(dumpSubredditPosts, tuple( product(subreddits, keyWords) ) ) 
    pool.close()
    pool.join()

    #Dedupe
    for s in subreddits:
        try:    records = pd.read_csv(f'{s}/{s}.csv', names = ["author","subreddit", "id", "title", "time","score","num_comments","domain"])
        except: continue
        deduped = records.drop_duplicates(["id"])
        deduped.to_csv(f'{s}/_{s}.csv', index=False)
        
