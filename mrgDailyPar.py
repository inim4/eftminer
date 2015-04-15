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

fileName = 'dat%(i)02d'
names = [fileName % {'i':i} for i in range(1,8)]
strDate = '2013-09-%(j)02d' 
dates = [strDate %{'j':j} for j in range(5,12)]

# function to summarize daily data
def prepDaily(dataName, strdate, week):

	#dat = readData(dataName)

	# --------------------------
	# function selectData(arg1,arg2,nWeek)
	# nWeek is manually added
	slc = selectData(dataName,strdate,week)
	trans = selectTrans(slc)
	returnDat = trans[1]
	arrTrans = frameTrans(trans)

	# --------------------------
	# resulting 16 attributes from original transaction dataset
	# 'strretailerid','sic','strdate','dt','daytime','nWeek','acqfiid','cardfiid', 'strcardno','tc','t','aa','c','famt1','famt2', 'famt'
	datTrans = arrTrans[0]

	# --------------------------
	# resulting 9 total transaction and monetary attributes
	# 'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17'
	dfTrans = arrTrans[1]

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
	dfRev = computeRevenue(datTrans,week)
	
	# daily data
	# resulting 29 attributes from 4 data frames
	# 'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17', 'nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6', 'nreturn', 'amtreturn', 'dt','daytime','nWeek','ntrans','amtRev','maxRev'
	arrDaily = [dfTrans,dfFiid,dfReturn,dfRev] 
	datDaily = aggAll(arrDaily)

	return datDaily
			

# Connect to the IPython cluster
# set 7 cluster engines, 1 cluster preprocess 1 daily data (total 7 daily dataset per week)
c = Client(profile='titaClusters')
c[:].run('prep.py')
c[:].block = True
numC = len(c)

day1 = c[0].apply_sync(prepDaily,names[0],dates[0],1)
day2 = c[1].apply_sync(prepDaily,names[1],dates[1],1)
day3 = c[2].apply_sync(prepDaily,names[2],dates[2],1)
day4 = c[3].apply_sync(prepDaily,names[3],dates[3],1)
day5 = c[4].apply_sync(prepDaily,names[4],dates[4],1)
day6 = c[5].apply_sync(prepDaily,names[5],dates[5],1)
day7 = c[6].apply_sync(prepDaily,names[6],dates[6],1)

# Array list for week 1 dataset, week number is set manually (e.g. datList1, datList2,...)
# consists of 7 daily data, 1 week = 7 days
datList01 = [day1,day2,day3,day4,day5,day6,day7]




