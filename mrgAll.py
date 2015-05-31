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


# --------------------------------------------------------------
# function to compute all 6 weeks slope
# --------------------------------------------------------------
def get6WSlope(grp6W):

	

	# Connect to the IPython cluster
	c = Client(profile='titaClusters')
	c[:].run('prep.py')
	c[:].block = True
	numC = len(c)
	nIntv = len(grp6W)/15
	cGroup = []

	for l in range(0,numC):
		m = l * nIntv
		n = (l+1) * nIntv
		if l == 14:
			lastInd = len(grp6W)
		else:
			lastInd = n
		indGrp = []
		for o in range(m,lastInd):
			#if grouped dataset has greater than 1 record (so the slope can be calculated - at least 2 data points)
			if len(grp6W[o]) > 1:
				indGrp.append(grp6W[o])
		cGroup.append(indGrp)

	#parallel calculation of all slope
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
	dfSlope['slopeInfo']=dfSlope['slope'].apply(lambda x: 1 if x > 0 else (0 if x == 0 else 2))

	df6wSlope = dfSlope[['rid','sic','slopeInfo','gradient','intercept']]
	
	del datSorted,datGrp,grp6W,cGroup,indGrp,dfTheta1,dfTheta2,dfTheta3,dfTheta4,dfTheta5,dfTheta6,dfTheta7,dfTheta8,dfTheta9,dfTheta10,dfTheta11,dfTheta12,dfTheta13,dfTheta14,dfTheta15,arrSlope,dfSlope

	return df6wSlope

#------------------------------------------
#1
#processing aggregated fortnightly data to summarize all 6w data
dat6w = pd.concat(aggFortnightList, ignore_index=True)
#aggregated fortnightly data 
#resulting 25 attributes: 'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6','ntrans','amtRev', 'nreturn', 'amtreturn'	
df6w = sumData(dat6w)

#------------------------------------------
#2
#get max revenue within 6 w data
#resulting 3 attributes: 'rid', 'sic', 'maxRev'
dfMaxRev = getMaxRevenue(dat6w)

#------------------------------------------
#3
#processing fortnightly data
datCombineCP = pd.concat(arrCombineCP, ignore_index=True)
#get mode of combination
#resulting 3 attributes: 'rid', 'sic', 'combineSlope'
dfCombine = datCombineCP.groupby(['rid','sic'])
grp6w=[]
for name, group in dfCombine:
	grp6w.append(group)
dfModeCombineCP = getModeCombineCP(grp6w)

#------------------------------------------
#4
#mode of week when max revenue occurs
#resulting attributes : 'rid', 'sic', 'weekMaxRev'
datWeekMaxRev = pd.concat(arrWeekMaxRev, ignore_index=True)
dfWeekMaxRev = getModeWeek(datWeekMaxRev)

#------------------------------------------
#5
#processing daily data
#resulting 5 attributes of 6w slope: 'rid','sic','slopeInfo','gradient','intercept'
dat6wSlope = pd.concat(dailyList, ignore_index=True)
slopeGrp = dat6wSlope.groupby(['rid','sic'])
grp6w=[]
for name, group in slopeGrp:
	grp6w.append(group)
df6wSlope = get6WSlope(grp6w)

#------------------------------------------
#6
#get mode of daytime when max revenue occurs
#resulting attributes: 'rid', 'sic', 'timeMaxRev'
dfDaytime = pd.concat(timeList, ignore_index=True)
dfModeTime = getModeTime(dfDaytime)

#merge all dataframes
df1 =  pd.merge(df6w,dfMaxRev,how='outer',on=['rid','sic'])
df2 =  pd.merge(df1,dfModeCombineCP,how='outer',on=['rid','sic'])
df3 =  pd.merge(df2,dfWeekMaxRev,how='outer',on=['rid','sic'])
df4 =  pd.merge(df3,df6wSlope,how='outer',on=['rid','sic'])
dfAll =  pd.merge(df4,dfModeTime,how='outer',on=['rid','sic'])

dfAll['ntc10']=dfAll['ntc10'].fillna(0)
dfAll['amttc10']=dfAll['amttc10'].fillna(0)
dfAll['ntc13']=dfAll['ntc13'].fillna(0)
dfAll['amttc13']=dfAll['amttc13'].fillna(0)
dfAll['ncashb']=dfAll['ncashb'].fillna(0)
dfAll['amtcashb']=dfAll['amtcashb'].fillna(0)
dfAll['ntc17']=dfAll['ntc17'].fillna(0)
dfAll['nreturn']=dfAll['nreturn'].fillna(0)
dfAll['amtreturn']=dfAll['amtreturn'].fillna(0)
dfAll['nfiid1']=dfAll['nfiid1'].fillna(0)
dfAll['nfiid2']=dfAll['nfiid2'].fillna(0)
dfAll['nfiid3']=dfAll['nfiid3'].fillna(0)
dfAll['nfiid4']=dfAll['nfiid4'].fillna(0)
dfAll['nfiid5']=dfAll['nfiid5'].fillna(0)
dfAll['nfiid6']=dfAll['nfiid6'].fillna(0)
dfAll['amtfiid1']=dfAll['amtfiid1'].fillna(0)
dfAll['amtfiid2']=dfAll['amtfiid2'].fillna(0)
dfAll['amtfiid3']=dfAll['amtfiid3'].fillna(0)
dfAll['amtfiid4']=dfAll['amtfiid4'].fillna(0)
dfAll['amtfiid5']=dfAll['amtfiid5'].fillna(0)
dfAll['amtfiid6']=dfAll['amtfiid6'].fillna(0)
dfAll['amtRev']=dfAll['amtRev'].fillna(0)
dfAll['ntrans']=dfAll['ntrans'].fillna(0)
dfAll['maxRev']=dfAll['maxRev'].fillna(0)
dfAll['initCont']=dfAll['initCont'].fillna(0)
dfAll['combineSlope']=dfAll['combineSlope'].fillna(0)
dfAll['slopeInfo']=dfAll['slopeInfo'].fillna(0)
dfAll['gradient']=dfAll['gradient'].fillna(0)
dfAll['intercept']=dfAll['intercept'].fillna(0)

dfAll['meanRev']= (dfAll['amtRev'] / dfAll['ntrans'])
minMeanRev = dfAll['meanRev'].min()
maxMeanRev = dfAll['meanRev'].max()
dfAll['normMeanRev']=(dfAll['meanRev']-minMeanRev)/(maxMeanRev-minMeanRev)
minMaxRev = dfAll['maxRev'].min()
maxMaxRev = dfAll['maxRev'].max()
dfAll['normMaxRev']=(dfAll['maxRev']-minMaxRev)/(maxMaxRev-minMaxRev)

del df6w,dfMaxRev,dfModeCombineCP,dfWeekMaxRev,df6wSlope,dfModeTime,timeList,dailyList,dfDaytime,dat6wSlope,datWeekMaxRev,datCombineCP,dat6w


