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
2 types of data:
- aggregated fortnightly data --> aggregated attributes: number + amount of transaction types; fiid; max revenue; amount revenue, number of transactions ()
- weekly data --> mode of week when max revenue occurs
- fortnightly data --> modeof combination of 2w slope and change points
- daily data in 6 W periode --> slope (intercept, gradient, slope info), mode of time of day
'''

#processing aggregated fortnightly data to summarize all 6w data
dat6w = pd.concat(aggFortnightList, ignore_index=True)
#aggregated fortnightly data 	
df6w = sumData(dat6w)

#processing fortnightly data
datCombineCP = pd.concat(arrCombineCP, ignore_index=True)
#get mode of combination
dfModeCombineCP = getModeCombineCP(datCombineCP)

#processing weekly data
datWeekly = pd.concat(arrCombineCP, ignore_index=True)

#processing daily data



