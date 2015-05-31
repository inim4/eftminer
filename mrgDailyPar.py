from IPython.parallel import Client
import pandas as pd
import numpy as np
import math
import time
import datetime
from datetime import datetime
from timeit import default_timer as clock
import gc
from sklearn.linear_model import LinearRegression
from prep import *


strDate = '2013-10-%(j)02d' 
dates = [strDate %{'j':j} for j in range(10,17)]


# function to summarize daily data
def prepDaily(dayData, dayDate, dayWeek):

	

	# --------------------------
	# function selectData(arg1,arg2,nWeek)
	# nWeek is manually added
	slc = selectData(dayData,dayDate,dayWeek)
	trans = selectTrans(slc)
	dfDT = getDayTime(trans)
	dfsic = filterSic(dfDT)
	dat = dfsic[0]
	returnDat = dfsic[1]
	rev = frameTrans(dat)

	# --------------------------
	# resulting 16 attributes from original transaction dataset
	# 'strretailerid','sic','strdate','dt','daytime','nWeek','acqfiid','cardfiid', 'strcardno','tc','t','aa','c','famt1','famt2', 'famt'
	datTrans = rev[0]

	# --------------------------
	# resulting 9 total transaction and monetary attributes
	# 'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17'
	dfTrans = rev[1]

	# --------------------------
	# resulting 4 attributes
	# 'rid', 'sic', 'nreturn', 'amtreturn'
	dfReturn = computeReturn(datTrans,returnDat)

	# --------------------------
	# resulting 14 attributes
	# 'rid','sic','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6'
	dfFiid = computeFiid(datTrans)

	# --------------------------
	# computeRevenue(datTrans,nWeek), nWeek is manually added
	# resulting 8 attributes
	# 'rid','sic','dt','daytime','nWeek','ntrans','amtRev','maxRev'
	dfRev = computeRevenue(datTrans,dayWeek)
	
	# daily data
	# resulting 29 attributes from 4 data frames
	# 'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17', 'nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6', 'nreturn', 'amtreturn', 'dt','daytime','nWeek','ntrans','amtRev','maxRev'
	arrDaily = [dfTrans,dfFiid,dfReturn,dfRev] 
	datDaily = aggAll(arrDaily)

	return datDaily
			

# Connect to the IPython cluster
# set 7 cluster engines, 1 cluster preprocess 1 daily data (total 7 daily dataset per week)
'''
c = Client(profile='titaClusters')
c[:].run('prep.py')
c[:].block = True
numC = len(c)

day01 = c[0].apply_sync(prepDaily,datWeek01[0],dates[0],1)
day02 = c[1].apply_sync(prepDaily,datWeek01[1],dates[1],1)
day03 = c[2].apply_sync(prepDaily,datWeek01[2],dates[2],1)
day04 = c[3].apply_sync(prepDaily,datWeek01[3],dates[3],1)
day05 = c[4].apply_sync(prepDaily,datWeek01[4],dates[4],1)
day06 = c[5].apply_sync(prepDaily,datWeek01[5],dates[5],1)
day07 = c[6].apply_sync(prepDaily,datWeek01[6],dates[6],1)


slc01 = c[0].apply_sync(selectData,datWeek01[0],dates[0],1)
slc02 = c[1].apply_sync(selectData,datWeek01[1],dates[1],1)
slc03 = c[2].apply_sync(selectData,datWeek01[2],dates[2],1)
slc04 = c[3].apply_sync(selectData,datWeek01[3],dates[3],1)
slc05 = c[4].apply_sync(selectData,datWeek01[4],dates[4],1)
slc06 = c[5].apply_sync(selectData,datWeek01[5],dates[5],1)
slc07 = c[6].apply_sync(selectData,datWeek01[6],dates[6],1)
'''


day14 = prepDaily(dat14,dates[6],2)
# consists of 7 daily data, 1 week = 7 days
#week01 = [day01,day02,day03,day04,day05,day06,day07]




