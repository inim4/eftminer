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
aggFortnightList=[]

#array for fortnightly data
arr1=[weekList[0],weekList[1]]
arr2=[weekList[2],weekList[3]]
arr3=[weekList[4],weekList[5]]
arrFortnight= [arr1,arr2,arr3]

arrSlope1=[dailyList[0],dailyList[1]]
arrSlope2=[dailyList[2],dailyList[3]]
arrSlope3=[dailyList[4],dailyList[5]]
arrSlope= [arrSlope1,arrSlope2,arrSlope3]

def get2WSlope(dat2wSlope):

	datSorted = dat2wSlope.sort_index(by='dt', ascending = True)
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
	df2wSlope = dfSlope[['rid','sic','slopeInfo']]

	return df2wSlope

for i in range(len(arrFortnight)):
	dat2w = pd.concat(arrFortnight[i], ignore_index=True)
	datCont = dat2w[['rid','sic','nWeek','amtRev']]
	slopeInfo = dat2w[['rid','sic','nWeek','slopeInfo']]
	
	if i == 0:
		#compute initial contribution -- only for the first fortnight dataset
		dfCont = getInitCont(datCont)
	
	#get ratio between first week and second week contribution
	dfContRatio = getContRatio(datCont)

	#get change point of weekly slope within fortnightly dataset
	dfChangePoint = getChangePoint(slopeInfo)
	
	#get 2 weeks slope
	datSlope = pd.concat(arrSlope[i], ignore_index=True)
	dat2wSlope = datSlope[['rid','sic','dt','amtRev']]
	df2wSlope = get2WSlope(dat2wSlope)

	#combine 2w slope and change point
	dfCombineCP = combineChangePoint(dfChangePoint,df2wSlope)
	
	#aggregated fortnightly data 	
	df2w = sumData(dat2w)

	#merge all
	mrg1 = pd.merge(dfCont,dfContRatio,how='outer',on=['rid','sic'])
	mrg2 = pd.merge(mrg1,dfCombineCP,how='outer',on=['rid','sic'])
	dfFortnightly = pd.merge(mrg2,df2w,how='outer',on=['rid','sic'])
	dfFortnightly['slopeInfo']=dfFortnightly['slopeInfo'].fillna(0)

	aggFortnightList.append(dfFortnightly)

	
	




