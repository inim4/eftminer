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



#array for ratio contribution per fortnight
arrContRatio=[]

#array for combination of change point & 2w slope per fortnight
arrCombineCP=[]


# --------------------------------------------------------------
# function to compute fortnightly slope
# --------------------------------------------------------------
def get2WSlope(dat2wSlope):

	datSorted = dat2wSlope.sort_index(by='dt', ascending = True)
	datGrp = datSorted.groupby(['rid','sic'])
	
	grp2W=[]
	for name, group in datGrp:
		grp2W.append(group)

	# Connect to the IPython cluster
	c2 = Client(profile='titaClusters2')
	c2[:].run('prep.py')
	c2[:].block = True
	numC2 = len(c2)
	nIntv = len(grp2W)/12
	cGroup = []

	for l in range(0,numC2):
		m = l * nIntv
		n = (l+1) * nIntv
		if l == 11:
			lastInd = len(grp2W)
		else:
			lastInd = n
		indGrp = []
		for o in range(m,lastInd):
			#if grouped dataset has greater than 1 record (so the slope can be calculated - at least 2 data points)
			if len(grp2W[o]) > 1:
				indGrp.append(grp2W[o])
		cGroup.append(indGrp)

	#parallel calculation of weekly slope
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
	dfTheta11 = c2[10].apply_sync(getSlope,cGroup[10])
	dfTheta12 = c2[11].apply_sync(getSlope,cGroup[11])
	

	arrSlope = [dfTheta1,dfTheta2,dfTheta3,dfTheta4,dfTheta5,dfTheta6,dfTheta7,dfTheta8,dfTheta9,dfTheta10,dfTheta11,dfTheta12]
	dfSlope = pd.concat(arrSlope, ignore_index=True)
	dfSlope['gradient']=dfSlope['slope'].apply(lambda x:round(math.degrees(np.arctan(x)),2))
	# slope positive = 1, slope negative = 2 
	dfSlope['slopeInfo']=dfSlope['slope'].apply(lambda x: 1 if x > 0 else 2)

	df2wSlope = dfSlope[['rid','sic','slopeInfo']]

	return df2wSlope


def prepFortnightly(arrFortnight):

	dat2w = pd.concat(arrFortnight, ignore_index=True)
	trans2w = dat2w[['rid','sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6','ntrans','amtRev', 'nreturn', 'amtreturn']]
	rev2w = dat2w[['rid','sic','nWeek','maxRev']]
	datCont = dat2w[['rid','sic','nWeek','amtRev']]
	slopeInfo = dat2w[['rid','sic','nWeek','weeklySlope']]

	#------------------------------------------
	#--1
	#compute initial contribution -- only for the first fortnight dataset
	#resulting 3 attributes: 'rid', 'sic', 'initCont'
	if i == 0:
		dfCont = getInitCont(datCont)
	
	#------------------------------------------
	#--2
	#get ratio between first week and second week contribution
	#resulting 3 attributes: 'rid', 'sic', 'contRatio'
	dfContRatio = getContRatio(datCont)
	arrContRatio.append(dfContRatio)

	#get change point of weekly slope within fortnightly dataset
	#resulting 3 attributes: 'rid', 'sic', 'changePoint'
	dfChangePoint = getChangePoint(slopeInfo)
	
	#get 2 weeks slope
	#resulting 3 attributes: 'rid', 'sic', 'slopeInfo'
	datSlope = pd.concat(arrSlope[i], ignore_index=True)
	dat2wSlope = datSlope[['rid','sic','dt','amtRev']]
	df2wSlope = get2WSlope(dat2wSlope)
	
	#------------------------------------------
	#--3
	#combine 2w slope and change point
	#resulting 3 attributes: 'rid', 'sic', 'grpCombineSlope'
	dfCombineCP = combineChangePoint(dfChangePoint,df2wSlope)
	arrCombineCP.append(dfCombineCP)
	
	#------------------------------------------
	#--4
	#aggregated fortnightly data 
	#resulting 25 attributes: 
	#'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6','ntrans','amtRev', 'nreturn', 'amtreturn'
	df2w = sumData(trans2w)

	#------------------------------------------
	#--5
	#get max revenue in 2 weeks
	#resulting 6 attributes:
	#'rid','sic','maxRev','nWeek'
	df2wMaxRev = fortnightMaxRevenue(rev2w)

	#------------------------------------------
	#merge data frame
	#resulting 28 attributes:
	#'rid', 'sic', 'initCont','ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6','ntrans','amtRev', 'nreturn', 'amtreturn','maxRev','nWeek'
	mrg = pd.merge(dfCont,df2w,how='outer',on=['rid','sic'])
	dfFortnightly = pd.merge(mrg,df2wMaxRev,how='outer',on=['rid','sic'])

	return dfFortnightly
	


#array for grouping fortnightly data
arr1=[weekList[0],weekList[1]]
arr2=[weekList[2],weekList[3]]
arr3=[weekList[4],weekList[5]]
arrFortnight= [arr1,arr2,arr3]

#array for grouping fortnightly slope data
arrSlope1 =[dailyList[0],dailyList[1]]
arrSlope2 =[dailyList[2],dailyList[3]]
arrSlope3 =[dailyList[4],dailyList[5]]
arrSlope =[arrSlope1,arrSlope2,arrSlope3]



# Connect to the IPython cluster
# set 3 sub-cluster engines, 1 cluster preprocess 1 weekly data (total 3 fortnightly dataset)
c1 = Client(profile='titaClusters1')
c1[:].run('prep.py')
c1[:].block = True
numC1 = len(c1)

datF1 = c1[0].apply_sync(prepFortnightly,arrFortnight[0])
datF2 = c1[1].apply_sync(prepFortnightly,arrFortnight[1])
datF3= c1[2].apply_sync(prepFortnightly,arrFortnight[2])

#array of summarized weekly data per fortnight
aggFortnightList=[datF1,datF2,datF3]

	
	




