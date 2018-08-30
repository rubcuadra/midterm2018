import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pandas as pd
import os.path
from collections import Counter
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
import matplotlib.collections as collections
from matplotlib.dates import (YEARLY, MONTHLY, DateFormatter,rrulewrapper, RRuleLocator, drange)
from scipy.interpolate import interp1d

subreddits = ["politics","news","The_Donald","truenews","PoliticalHumor","democrats","all"] #"ElectionPolls", "Ask_Politics"

for subreddit in subreddits:
    c = Counter()
    fName = f'{subreddit}/_{subreddit}.csv'
    if not os.path.isfile(fName) : continue
    records = pd.read_csv(fName, names = ["author","subreddit", "id", "title", "time","score","num_comments","domain"])
    #Count on same day
    for index, row in records.iterrows():
        if index==0: continue #Headers
        c[ datetime.strptime(row["time"],'%Y-%m-%d %H:%M:%S').strftime("%y/%m/%d")] += 1
    #Convert to numpy
    temp = np.array([ [datetime.strptime(r,"%y/%m/%d"), c[r]] for r in c ] )
    temp = temp[temp[:,0].argsort()] #Sort

    #Start plot
    plt.style.use('ggplot')
    fig, ax = plt.subplots()
    ax.xaxis_date()
    ax.xaxis.set_major_locator(RRuleLocator(rrulewrapper(MONTHLY, bymonthday=15)))
    ax.xaxis.set_major_formatter(DateFormatter('%m/%d/%Y'))
    ax.xaxis.set_tick_params(rotation=90, labelsize=8)

    #Plot with smoothness
    # dateTimeToNumber = np.vectorize( lambda dt: (dt - datetime(1970,1,1)).total_seconds() )
    # numberToDateTime = np.vectorize( lambda num: datetime.utcfromtimestamp(num)  )
    # numeric_x = dateTimeToNumber(temp[:,0]  )
    # f = interp1d( numeric_x, temp[:,1] , kind='nearest')
    # x_range = np.linspace(numeric_x[0], numeric_x[-1],500)
    # y_smooth= f(x_range)
    # plt.plot ( numberToDateTime(x_range) ,y_smooth, "b",linewidth=2 )
    
    #Plot Lines
    # plt.plot( temp[:,0],temp[:,1], "o-", markersize=1, linewidth=0.5 )
    
    #Plot bars
    plt.bar( temp[:,0],temp[:,1], width=1 )

    #Draw region >2018
    # ax.fill_between(temp[:,0], 0, temp[:,1].max(), where=temp[:,0]>datetime(2018,1,1), facecolor='green', alpha=0.25)
    #Axes and labels
    # plt.xlabel('mm/dd/yy')
    plt.ylabel('# Posts')
    plt.title(f'# of Posts related with latinos/hispanic in r/{subreddit}')
    plt.grid(True)
    plt.tight_layout()
    
    #Plot
    # plt.show()
    plt.savefig(f'{subreddit}.png')
    

