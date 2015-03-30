import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sklearn



dat = prep1[['strretailerid','sic','dt', 'famt']]
datSorted = dat.sort_index(by='dt', ascending = True)
datGrp=datSorted.groupby(['strretailerid','sic'])
#slopeDat = pd.DataFrame({'rid':[],'sic':[], 'intercept':[], 'slope':[]})
arrName=[]
tmpArr=[]
theta=[]

for name, group in datGrp:
	arrName.append(name)

# Connect to the IPython cluster
from IPython.parallel import Client
c = Client(profile='titaClusters')
n = len(arrName)/4
for i in range(0,len(c)):
	j = i * n
	k = (i+1) * n
	if i == 3:
		lastInd = len(arrName)
	else:
		lastInd = k
	for l in range(j,lastInd):
		indGrp = arrName[l]
		tmpArr = datGrp.get_group(indGrp)
		tmpArr = tmpArr.set_index(['strretailerid','sic'])
		if len(tmpArr) > 1:
			dfTmp = pd.DataFrame({'rid':[tmpArr.index[m][0] for m in range(0,len(tmpArr))],'sic':[tmpArr.index[m][1] for m in range(0,len(tmpArr))], 'dt':[tmpArr['dt'][m] for m in range(0,len(tmpArr))], 'famt':[tmpArr['famt'][m] for m in range(0,len(tmpArr))]})
			dfTmpSorted = dfTmp.sort_index(by='dt', ascending=True)
			dfTmpSorted['sorted']=[o for o in range(0,len(dfTmpSorted))]
			x = dfTmpSorted['sorted']
			y = dfTmpSorted['famt']
			tmp1=[]
			tmp2=[]
			for p in range(0,len(x)):
				tmp1.append(x[p])
			for q in range(0,len(y)):
				tmp2.append(y[q])

			datX = np.array(tmp1)
			datY = np.array(tmp2)
			from sklearn.linear_model import LinearRegression
			lm = LinearRegression()
			lm.fit(datX[:,np.newaxis],datY)
			intercept = lm.intercept_
			slope = lm.coef_[0]
			dfTheta = pd.DataFrame({'rid':(dfTmpSorted['rid'][0]),'sic':(dfTmpSorted['sic'][0]), 'intercept':[intercept], 'slope':[slope]})
			slopeDat = slopeDat.append(dfTheta, ignore_index=True)
		else:
			continue
