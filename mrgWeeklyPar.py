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



#array list of daily data per week 
#length of array = 6 -> 6 weeks
#every array item (index) consists of 
#dailyList - for computing slope in 6 weeks
#timeList - for computing daytime when max revenue occurs (timeMaxRev)
dailyList=[]
timeList=[]


def getWeeklySlope(weeklyGrp):

	# Connect to the IPython cluster
	# Start 10 sub-cluster engines
	c2 = Client(profile='titaClusters2')
	c2[:].run('prep.py')
	c2[:].block = True
	numC2 = len(c2)
	nIntv = len(weeklyGrp)/10
	cGroup = []

	for l in range(0,numC2):
		m = l * nIntv
		n = (l+1) * nIntv
	if l == 9:
		lastInd = len(weeklyGrp)
	else:
		lastInd = n
	indGrp = []
	for o in range(m,lastInd):
		if len(weeklyGrp[o]) > 1:
			indGrp.append(weeklyGrp[o])
	cGroup.append(indGrp)

	#parallel calculation of weekly slope using 15 cluster engines
	dfTheta1 = c2[0].apply_sync(getSlope,cGroup[0])
	dfTheta2 = c2[1].apply_sync(getSlope,cGroup[1])
	dfTheta3 = c2[2].apply_sync(getSlope,cGroup[2])
	dfTheta4 = c2[3].apply_sync(getSlope,cGroup[3])
	dfTheta5 = c2[4].apply_sync(getSlope,cGroup[4])
	dfTheta6 = c2[5].apply_sync(getSlope,cGroup[5])
	dfTheta7 = c2[6].apply_sync(getSlope,cGroup[6])
	dfTheta8 = c2[7].apply_sync(getSlope,cGroup[7])
	dfTheta9 = c2[8].apply_sync(getSlope,cGroup[8])
	dfTheta10 = c2[9].apply_sync(getSlope,cGroup[9])

	arrSlope = [dfTheta1,dfTheta2,dfTheta3,dfTheta4,dfTheta5,dfTheta6,dfTheta7,dfTheta8,dfTheta9,dfTheta10]
	dfSlope = pd.concat(arrSlope, ignore_index=True)

	dfSlope['gradient']=dfSlope['slope'].apply(lambda x:round(math.degrees(np.arctan(x)),2))
	# slope positive = 1, slope negative = 2 
	dfSlope['slopeInfo']=dfSlope['slope'].apply(lambda x: 1 if x > 0 else 2)

	return dfSlope



def prepWeekly(arrDaily, nWeek):


	#------------------------------------------
	#daily data per week are concatenated into one
	dfOneWeek = pd.concat(arrDaily, ignore_index=True)

	#------------------------------------------
	#--1
	#aggregated (summarize) weekly data
	#resulting 25 attributes: 
	#'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6','ntrans','amtRev', 'nreturn', 'amtreturn'
	dfWeeklyDat = sumData(dfOneWeek)

	#------------------------------------------
	#--2
	#get max revenue per week
	#resulting 6 attributes:
	#'rid','sic','maxRev','nWeek'
	dfWeeklyMaxRev = weeklyMaxRevenue(dfOneWeek, nWeek)

	#------------------------------------------
	#extract relevant attributes for generating 6w slope from daily data and daytime when max revenue occurs 
	datSlope = dfOneWeek[['rid','sic','dt','amtRev']]
	datTime = dfOneWeek[['rid','sic','daytime']]
	#store in array list
	dailyList.append(datSlope)
	timeList.append(datTime)

	#------------------------------------------
	#--3
	#dataframe of weekly slope
	#resulting 3 attributes: 'rid','sic','weeklySlope'
	datSorted = datSlope.sort_index(by='dt', ascending = True)
	datGrp = datSorted.groupby(['rid','sic'])
	weeklyGrp=[]
	for name, group in datGrp:
		weeklyGrp.append(group)
	dfSlope = getWeeklySlope(weeklyGrp)
	dfSlope['weeklySlope']=dfSlope['slopeInfo']
	dfWeeklySlope = dfSlope[['rid','sic','weeklySlope']]

	#------------------------------------------
	#merge aggregated data frames
	#resulting 29 attributes:
	#'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6','ntrans','amtRev', 'nreturn', 'amtreturn','maxRev','nWeek','dt','daytime','weeklySlope'
	mrg = pd.merge(dfWeeklyDat,dfWeeklyMaxRev,how='outer',on=['rid','sic'])
	dfWeekly = pd.merge(mrg,dfWeeklySlope,how='outer',on=['rid','sic'])
	dfWeekly['slopeInfo']=dfWeekly['slopeInfo'].fillna(0)
	
	return dfWeekly




datName = 'datList%(i)02d'
arrDaily = [datName % {'i':i} for i in range(1,7)]

# Connect to the IPython cluster
# set 6 sub-cluster engines, 1 cluster preprocess 1 weekly data (total 6 weekly dataset)
c1 = Client(profile='titaClusters1')
c1[:].run('prep.py')
c1[:].block = True
numC1 = len(c1)

datWeek1 = c1[0].apply_sync(prepWeekly,arrDaily[0],1)
datWeek2 = c1[1].apply_sync(prepWeekly,arrDaily[1],2)
datWeek3 = c1[2].apply_sync(prepWeekly,arrDaily[2],3)
datWeek4 = c1[3].apply_sync(prepWeekly,arrDaily[3],4)
datWeek5 = c1[4].apply_sync(prepWeekly,arrDaily[4],5)
datWeek6 = c1[5].apply_sync(prepWeekly,arrDaily[5],6)


# Array list for week 1 dataset, week number is set manually (e.g. datList1, datList2,...)
# consists of 7 daily data, 1 week = 7 days
weekList = [datWeek1,datWeek2,datWeek3,datWeek4,datWeek5,datWeek6]