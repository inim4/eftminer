from IPython.parallel import Client
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from prep import *

#daily slope
datSlope = datTrans[['strretailerid','sic','dt', 'famt']]
datSorted = datSlope.sort_index(by='dt', ascending = True)
datGrp = datSorted.groupby(['strretailerid','sic'])
	
grpList = []
for name, group in datGrp:
	grpList.append(group)

#Connect to the IPython cluster
c = Client(profile='titaClusters')
c[:].run('prep.py')
c[:].block = True
numC = len(c)
v=c[:numC]	

nIntv = len(grpList)/4
cGroup = []

for l in range(0,numC):
	m = l * nIntv
	n = (l+1) * nIntv
	if l == 3:
		lastInd = len(grpList)
	else:
		lastInd = n
	indGrp = []
	for o in range(m,lastInd):
		if len(grpList[o]) > 1:
			indGrp.append(grpList[o])
	cGroup.append(indGrp)

#dfTheta = v.apply_sync(computeDailySlope,cGroup[:numC],datGrp1)

#parallel compute daily slope of retailer for one-day dataset
dfTheta1 = c[0].apply_async(computeDailySlope,cGroup[0])
'''
dfTheta2 = c[1].apply_sync(computeDailySlope,cGroup[1],datGrp1)
dfTheta3 = c[2].apply_sync(computeDailySlope,cGroup[2],datGrp1)
dfTheta4 = c[3].apply_sync(computeDailySlope,cGroup[3],datGrp1)
dfTheta5 = c[4].apply_sync(computeDailySlope,cGroup[4],datGrp1)
dfTheta6 = c[5].apply_sync(computeDailySlope,cGroup[5],datGrp1)
dfTheta7 = c[6].apply_sync(computeDailySlope,cGroup[6],datGrp1)
dfTheta8 = c[7].apply_sync(computeDailySlope,cGroup[7],datGrp1)
dfTheta9 = c[8].apply_sync(computeDailySlope,cGroup[8],datGrp1)
dfTheta10 = c[9].apply_sync(computeDailySlope,cGroup[9],datGrp1)

arrSlope = [dfTheta1,dfTheta2,dfTheta3,dfTheta4,dfTheta5,dfTheta6,dfTheta7,dfTheta8,dfTheta9,dfTheta10]
dfSlope = pd.concat(arrSlope, ignore_index=True)

pieces = [dfTrans,dfFiid,dfReturn,dfRev,dfSlope] 
dataprep = aggAll(pieces)
datList.append(dataprep)
'''
