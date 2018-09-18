from enum import Enum

class types(Enum):
    JSON = "J"
    CSV  = "C"

dumpType = types.CSV
subreddits = ["politics","news","The_Donald","truenews","PoliticalHumor","democrats","all"] #"ElectionPolls", "Ask_Politics"
keyWords   = ["latino", "hispanic","immigrant","realdonaldtrump","texas","daca","immigration","families","register","trump","children","immigrants",'dreamers'] #"misinformation", "midterm" 
oldest_post_date = "10/09/2017"  #dd/mm/yyyy
    

# if __name__ == '__main__':
#     dumpType = types.CSV
#     subreddits = ["politics","news","The_Donald","truenews","PoliticalHumor","democrats","all"] #"ElectionPolls", "Ask_Politics"
#     keyWords   = ["latino", "hispanic","immigrant","realdonaldtrump","texas","daca","immigration","families","register","trump","children","immigrants",'dreamers'] #"misinformation", "midterm" 
#     # keyWords   = ["latino", "hispanic"] #"misinformation", "midterm" 
#     oldest_post_date = "10/09/2017"  #dd/mm/yyyy
    
#     #Create CSV files with posts
#     # scrapper(subreddits,keyWords,oldest_post_date,dumpType, filter_text=True)
#     #Create .png files from the CSV files
#     # chart(subreddits)
#     