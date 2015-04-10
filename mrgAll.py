from IPython.parallel import Client
import pandas as pd
import numpy as np
import math
import time
import datetime
from datetime import datetime
from timeit import default_timer as clock
from sklearn.linear_model import LinearRegression
from prep import *


'''
resulting data:
- aggregated fortnightly data --> aggregated attributes: number + amount of transaction types; fiid; max revenue; amount revenue, number of transactions ()
- fortnightly data --> modeof combination of 2w slope and change points
- weekly data --> mode of week when max revenue occurs
- daily data in 6 W periode --> slope (intercept, gradient, slope info), mode of time of day
'''


#1
#processing aggregated fortnightly data to summarize all 6w data
dat6w = pd.concat(aggFortnightList, ignore_index=True)
#aggregated fortnightly data 	
df6w = sumData(dat6w)

#2
#processing fortnightly data
datCombineCP = pd.concat(arrCombineCP, ignore_index=True)
#get mode of combination
dfModeCombineCP = getModeCombineCP(datCombineCP)

#3
#processing weekly data
datWeekly = pd.concat(arrCombineCP, ignore_index=True)

#processing daily data
datSlope6w = pd.concat(dailyList, ignore_index=True)
datSorted6w = datSlope6w.sort_index(by='dt', ascending = True)
datGrp6w = datSorted6w.groupby(['rid','sic'])	
arrName6w=[]
tmpArr6w=[]
for name, group in datGrp6w:
	arrName6w.append(name)
# Connect to the IPython cluster
c[:].run('prep.py')
c[:].block = True
nIntv = len(arrName6w)/10
cGroup = []
#get mode of daytime when max revenue occurs
dfDaytime = pd.concat(timeList, ignore_index=True)
dfModeTime = getModeTime(dfDaytime)


