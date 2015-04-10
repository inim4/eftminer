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
#array list of daily data with weekly format: 6 data items (6w). 1 data item consists of concatenated daily data within a week
dailyList=[]
timeList=[]

datName = 'datList%(i)02d'
arrDaily = [datName % {'i':i} for i in range(1,8)]

for j in range(len(arrDaily)):
	#merge daily data into weekly
	dfOneWeek = pd.concat(arrDaily[j], ignore_index=True)
	#aggregated attributes. dt, daytime, and max revenue
	dfWeekly = sumData(dfOneWeek)

	#extract relevant attributes for generating 6w slope from daily data and get weekly slope
	datSlope = dfOneWeek[['rid','sic','dt', 'amtRev']]
	datTime = dfOneWeek[['rid','sic','daytime']]
	dailyList.append(datSlope)
	timeList.append(datTime)

	#weekly slope
	datSorted = datSlope.sort_index(by='dt', ascending = True)
	datGrp = datSorted.groupby(['rid','sic'])
	
	arrName=[]
	tmpArr=[]
	theta=[]

	for name, group in datGrp:
		arrName.append(name)

	# Connect to the IPython cluster
	c = Client(profile='titaClusters')
	c[:].run('prep.py')
	c[:].block = True
	nIntv = len(arrName)/10
	cGroup = []

	for l in range(0,len(c)):
		m = l * nIntv
		n = (l+1) * nIntv
		if l == 9:
			lastInd = len(arrName)
		else:
			lastInd = n
		indGrp = []
		for o in range(m,lastInd):
			tmpArr = datGrp.get_group(arrName[o])
			#if grouped dataset has greater than 1 record (so the slope can be calculated - at least 2 data points)
			if len(tmpArr) > 1:
				#temporary array -- append index group for one cluster engine
				indGrp.append(arrName[o])
		#store index group for each cluster engine
		cGroup.append(indGrp)

	#parallel calculation of weekly slope
	dfTheta1 = c[0].apply_sync(getSlope,cGroup[0],datGrp)
	dfTheta2 = c[1].apply_sync(getSlope,cGroup[1],datGrp)
	dfTheta3 = c[2].apply_sync(getSlope,cGroup[2],datGrp)
	dfTheta4 = c[3].apply_sync(getSlope,cGroup[3],datGrp)
	dfTheta5 = c[4].apply_sync(getSlope,cGroup[4],datGrp)
	dfTheta6 = c[5].apply_sync(getSlope,cGroup[5],datGrp)
	dfTheta7 = c[6].apply_sync(getSlope,cGroup[6],datGrp)
	dfTheta8 = c[7].apply_sync(getSlope,cGroup[7],datGrp)
	dfTheta9 = c[8].apply_sync(getSlope,cGroup[8],datGrp)
	dfTheta10 = c[9].apply_sync(getSlope,cGroup[9],datGrp)

	arrSlope = [dfTheta1,dfTheta2,dfTheta3,dfTheta4,dfTheta5,dfTheta6,dfTheta7,dfTheta8,dfTheta9,dfTheta10]
	dfSlope = pd.concat(arrSlope, ignore_index=True)
	dfWeeklySlope = dfSlope[['rid','sic','slopeInfo']]

	#merge aggregated data frame and slope data
	dfWeekly = pd.merge(dfWeekly,dfWeeklySlope,how='outer',on=['rid','sic'])
	dfWeekly['slopeInfo']=dfWeekly['slopeInfo'].fillna(0)
	

	weekList.append(dfWeekly)
