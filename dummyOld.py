import requests
import csv
import sys
from os import cpu_count
from time import sleep
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing import Pool

subreddits = ["ElectionPolls","politics","news","The_Donald","truenews","PoliticalHumor","shittypolitics","shittynews","Voting","democrats","Ask_Politics"]
oldest_post_date = "01/01/2018"  #dd/mm/yyyy

def dumpAll(subreddit):
    url = f"https://old.reddit.com/r/{subreddit}/"   #url address to subreddit you want to scrape
    headers = {'User-Agent': 'Mozilla/5.0'}
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')
    attrs = {'class': 'thing'}

    oldest_pd = datetime.strptime(oldest_post_date, '%d/%m/%Y')
    last_post_date = datetime.today() #Just to start
    while oldest_pd<last_post_date: 
        for post in soup.find_all("div", attrs=attrs):
            title = str(post.find('a', class_='title').text.encode('ascii', 'ignore'))[2:-1]
            domain = post.find('span', class_='domain').text[1:-1]
            tempTime = post.find('time').get('title').split()
            time = tempTime[2]+' '+tempTime[1]+' '+tempTime[4]+' '+tempTime[3]
            likes = post.find('div', attrs={'class': "score unvoted"}).text
            comments = post.find('a', class_='comments').text.split()[0]

            if comments == "comment":
                comments = 0

            if likes == "•":
                likes = 0

            last_post_date = datetime.strptime(time, '%d %b %Y %H:%M:%S') #Update for while
            
            # print(time)
            # print(title)
            # print(domain)
            # print(time)
            # print(author)
            # print(likes)
            # print(comments)
            # print('=========================')

            post_line = [title, time, likes, comments]
            with open(f'all_{subreddit}.csv', 'a') as f:
                writer = csv.writer(f)
                writer.writerow(post_line)        
        sleep(10)
        try:
            next_button = soup.find('span', class_="next-button")
            next_page_link = next_button.find("a").get('href')
            page = requests.get(next_page_link, headers=headers)
            soup = BeautifulSoup(page.text, 'html.parser')
        except:
            print (next_page_link)
            break

if __name__ == '__main__':
    pool = Pool(cpu_count()) #Parallelize crawlers
    pool.map(dumpAll, subreddits ) 
    pool.close()
    pool.join()


    # https://old.reddit.com/r/news/?count=123&after=t3_9ah5bz
    # https://old.reddit.com/r/Ask_Politics/?count=750&after=t3_89c9f3
    # https://old.reddit.com/r/PoliticalHumor/?count=900&after=t3_99tw31
    # https://old.reddit.com/r/politics/?count=925&after=t3_9ajui8
    # https://old.reddit.com/r/democrats/?count=975&after=t3_94yc75
    # https://old.reddit.com/r/The_Donald/?count=950&after=t3_9aw4i6
