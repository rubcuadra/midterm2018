from enum import Enum

class types(Enum):
    JSON = "J"
    CSV  = "C"

dumpType = types.CSV
subreddits = ["politics","news","The_Donald","truenews","PoliticalHumor","democrats","Ask_Politics","ElectionPolls","republicans"] #"all"
keyWords   = ["latino", "hispanic","immigrant","realdonaldtrump","texas","daca","immigration","families","register","trump","children","immigrants",'dreamers'] #"misinformation", "midterm" 
oldest_post_date = "10/09/2017"  #dd/mm/yyyy
    

#Scripts order
#	scrapper.py
#	charts.py
#   analyze_data.py