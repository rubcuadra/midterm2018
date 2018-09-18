from os import path, makedirs, cpu_count, environ, remove
environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
from multiprocessing import Pool
from regex import compile, match
from itertools import product
from datetime import datetime
from requests import get
from time import sleep
from json import dump
import pandas as pd
import shutil
import csv
from config import types

def isPostValid(post):
    #example: if "something" in post["title"] ...
    #We can filter by keywords, otherwise we dump all posts
    #pprint(post)
    return True

def dumpSubredditPosts(subreddit, keyword, oldest_post_date, dumpType):
    # subreddit, keyword, oldest_post_date, dumpType = payload[0],payload[1], payload[2], payload[3] #Autogenerated Tuple
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
                        post_line = [post["data"]["author"],post["data"]["subreddit_name_prefixed"],post["data"]["id"],post["data"]["title"].replace(","," "), timeInIso, post["data"]["score"], post["data"]["num_comments"], post["data"]["domain"], post["data"]["url"], '.' if post["data"]["selftext"] in ['',' ',None] else post["data"]["selftext"].replace(',',' ').replace('\n',' ')]
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

def run(pool,subreddits,keyWords,opd,dumpType=types.CSV, filter_text=False):
    #Inputs
    tp = tuple( product(subreddits, keyWords, [opd], [dumpType]) )
    pool.starmap(dumpSubredditPosts, tp ) 
    pool.close()
    pool.join()

    #Dedupe
    for s in subreddits:
        try:    records = pd.read_csv(f'{s}/{s}.csv', names = ["author","subreddit", "id", "title", "time","score","num_comments","domain","url","selftext"])
        except: continue
        deduped = records.drop_duplicates(["id"])
        deduped.to_csv(f'{s}/temp_{s}.csv', index=False)
        shutil.move(f'{s}/temp_{s}.csv', f'{s}/{s}.csv')
    
    if filter_text: #Filter only if it has the Keyword in the title or selftext
        r = compile(f"^.*({'|'.join([k for k in keyWords])})+.*$") #Compile regex, faster
        for s in subreddits:
            try: #case we do not have the csv
                with open(f'{s}/{s}.csv',"r") as f,open(f'{s}/filtered_{s}.csv',"w+") as t:
                    reader = csv.DictReader(f)
                    t.write(f"{','.join(reader.fieldnames)}\n") #Write headers
                    for line in reader:
                        if r.match(line["title"]) or r.match(line["selftext"]): #Match regex
                            l = ",".join(line[fName] for fName in reader.fieldnames)
                            t.write(f"{l}\n")
                    # shutil.move(f'{s}/temp_{s}.csv', f'{s}/{s}.csv')
            except FileNotFoundError : 
                continue

if __name__ == '__main__':
    from config import subreddits,keyWords,oldest_post_date,dumpType
    pool = Pool(cpu_count()) #Parallelize crawlers
    dumpType = types.CSV
    run(pool, subreddits,keyWords,oldest_post_date,dumpType,filter_text=False)
    