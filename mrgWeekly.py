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


#array of weekly data
weekList=[]

#array list of daily data per week 
#length of array = 6 -> 6 weeks
#every array item (index) consists of 
#dailyList - for computing slope in 6 weeks
#timeList - for computing daytime when max revenue occurs (timeMaxRev)
dailyList=[]
timeList=[]


def getWeeklySlope(weeklyGrp):

	# Connect to the IPython cluster
	# Start 15 cluster engines
	c = Client(profile='titaClusters')
	c[:].run('prep.py')
	c[:].block = True
	numC = len(c)
	#grouped rid+sic are distributed to 15 engines (e.g. 1 engine processes 500 groups)
	nIntv = len(weeklyGrp)/15
	cGroup = []

	for l in range(0,numC):
		m = l * nIntv
		n = (l+1) * nIntv
		if l == 14:
			lastInd = len(weeklyGrp)
		else:
			lastInd = n
		indGrp = []
		#only process group with 2 or more instances
		for o in range(m,lastInd):
			if len(weeklyGrp[o]) > 1:
				indGrp.append(weeklyGrp[o])
		cGroup.append(indGrp)

	#parallel calculation of weekly slope using 15 cluster engines
	dfTheta1 = c[0].apply_sync(getSlope,cGroup[0])
	dfTheta2 = c[1].apply_sync(getSlope,cGroup[1])
	dfTheta3 = c[2].apply_sync(getSlope,cGroup[2])
	dfTheta4 = c[3].apply_sync(getSlope,cGroup[3])
	dfTheta5 = c[4].apply_sync(getSlope,cGroup[4])
	dfTheta6 = c[5].apply_sync(getSlope,cGroup[5])
	dfTheta7 = c[6].apply_sync(getSlope,cGroup[6])
	dfTheta8 = c[7].apply_sync(getSlope,cGroup[7])
	dfTheta9 = c[8].apply_sync(getSlope,cGroup[8])
	dfTheta10 = c[9].apply_sync(getSlope,cGroup[9])
	dfTheta11 = c[10].apply_sync(getSlope,cGroup[10])
	dfTheta12 = c[11].apply_sync(getSlope,cGroup[11])
	dfTheta13 = c[12].apply_sync(getSlope,cGroup[12])
	dfTheta14 = c[13].apply_sync(getSlope,cGroup[13])
	dfTheta15 = c[14].apply_sync(getSlope,cGroup[14])

	arrSlope = [dfTheta1,dfTheta2,dfTheta3,dfTheta4,dfTheta5,dfTheta6,dfTheta7,dfTheta8,dfTheta9,dfTheta10,dfTheta11,dfTheta12,dfTheta13,dfTheta14,dfTheta15]
	dfSlope = pd.concat(arrSlope, ignore_index=True)

	dfSlope['gradient']=dfSlope['slope'].apply(lambda x:round(math.degrees(np.arctan(x)),2))
	# slope positive = 1, slope negative = 2 
	dfSlope['slopeInfo']=dfSlope['slope'].apply(lambda x: 1 if x > 0 else 2)

	return dfSlope




#for j in range(len(week01)):

#------------------------------------------
#daily data per week are concatenated into one
dfOneWeek = pd.concat(week01, ignore_index=True)

#------------------------------------------
#aggregated (summarize) weekly data
#resulting 25 attributes: 
#'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6','ntrans','amtRev', 'nreturn', 'amtreturn'
dfWeeklyDat = sumData(dfOneWeek)

#------------------------------------------
#get max revenue per week
#resulting attributes:
#'rid','sic','maxRev','nWeek','dt','daytime'
dfWeeklyMaxRev = weeklyMaxRevenue(dfOneWeek, 1)

#------------------------------------------
#extract relevant attributes for generating 6w slope from daily data and daytime when max revenue occurs 
datSlope = dfOneWeek[['rid','sic','dt','amtRev']]
datTime = dfOneWeek[['rid','sic','daytime']]
#store in array list
dailyList.append(datSlope)
timeList.append(datTime)

#------------------------------------------
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
dfWeekly['weeklySlope']=dfWeekly['weeklySlope'].fillna(0)
	

#weekList.append(dfWeekly)
