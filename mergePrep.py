import pandas as pd
import numpy as np
import math
import time
import datetime
from datetime import datetime, date, timedelta
from numpy import zeros, ones, array, linspace, logspace
from pylab import scatter, show, title, xlabel, ylabel, plot, contour
from scipy.stats.mstats import mode

#head = ['rid','sic','ntc10','amttc10','ntc13','amttc13','ncashb','amtcashb', 'nreturn', 'amtreturn', 'ntc17','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3', 'amtfiid3', 'nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6', 'amtfiid6', 'amtRev', 'meanRev', 'maxRev', 'normAmtRev', 'normMeanRev', 'normMaxRev', 'timeOfDay', 'dt', 'dayIntercept', 'daySlope','dayGradient', 'daySlopeInfo']


#Evaluate the linear regression
def compute_cost(X, y, theta):
	'''
	Comput cost for linear regression
	'''
	#Number of training samples
	
	m=len(y)

	predictions = X.dot(theta).flatten()

	sqErrors = (predictions - y) ** 2

	J = (1.0 / (2 * m)) * sqErrors.sum()

	return J


def gradient_descent(X, y, theta, alpha, num_iters):
	'''
	Performs gradient descent to learn theta
	by taking num_items gradient steps with learning
	rate alpha
	'''

	m=len(y)	
	J_history = zeros(shape=(num_iters, 1))

	for i in range(num_iters):

		predictions = X.dot(theta).flatten()

		errors_x1 = (predictions - y) * X[:, 0]
		errors_x2 = (predictions - y) * X[:, 1]

		theta[0][0] = theta[0][0] - alpha * (1.0 / m) * errors_x1.sum()
		theta[1][0] = theta[1][0] - alpha * (1.0 / m) * errors_x2.sum()

		J_history[i, 0] = compute_cost(X, y, theta)

	return theta, J_history

def linReg(dataX, dataY):
	
    
	
	m=len(dataY)

	#Add a column of ones to X (interception data)
	it = ones(shape=(m, 2))
	it[:, 1] = dataX

	#Initialize theta parameters
	theta = zeros(shape=(2, 1))

	#Some gradient descent settings
	iterations = 1500
	alpha = 0.01

	#compute and display initial cost
	#print compute_cost(it, dataY, theta)

	theta, J_history = gradient_descent(it, dataY, theta, alpha, iterations)

	#print theta

	#Plot the results
	
	result = it.dot(theta).flatten()
       

	#Grid over which we will calculate J
	theta0_vals = linspace(-10, 10, 100)
	theta1_vals = linspace(-1, 4, 100)


	#initialize J_vals to a matrix of 0's
	J_vals = zeros(shape=(theta0_vals.size, theta1_vals.size))

	#Fill out J_vals
	for t1, element in enumerate(theta0_vals):
		for t2, element2 in enumerate(theta1_vals):
			thetaT = zeros(shape=(2, 1))
			thetaT[0][0] = element
			thetaT[1][0] = element2
			J_vals[t1, t2] = compute_cost(it, dataY, thetaT)

	
	
	J_vals = J_vals.T
        
	return theta

def groupingData(dfDat):
	
	# merge weekly data grouped by rid and sic
	'''
	sum all numerical variables
	'''
	# merge / sum transaction code and monetary values
	ntc10 = dfDat.groupby(['rid','sic'])['ntc10'].sum()
	amttc10 = dfDat.groupby(['rid','sic'])['amttc10'].sum()
	ntc13 = dfDat.groupby(['rid','sic'])['ntc13'].sum()
	amttc13 = dfDat.groupby(['rid','sic'])['amttc13'].sum()
	ncashb = dfDat.groupby(['rid','sic'])['ncashb'].sum()
	amtcashb = dfDat.groupby(['rid','sic'])['amtcashb'].sum()
	nreturn = dfDat.groupby(['rid','sic'])['nreturn'].sum()
	amtreturn = dfDat.groupby(['rid','sic'])['amtreturn'].sum()
	ntc17 = dfDat.groupby(['rid','sic'])['ntc17'].sum()
	amtRev = dfDat.groupby(['rid','sic'])['amtRev'].sum()
	ntrans = dfDat.groupby(['rid','sic'])['ntrans'].sum()

	# merge / sum fiid
	nfiid1 = dfDat.groupby(['rid','sic'])['nfiid1'].sum()
	amtfiid1 = dfDat.groupby(['rid','sic'])['amtfiid1'].sum()
	nfiid2 = dfDat.groupby(['rid','sic'])['nfiid2'].sum()
	amtfiid2 = dfDat.groupby(['rid','sic'])['amtfiid2'].sum()
	nfiid3 = dfDat.groupby(['rid','sic'])['nfiid3'].sum()
	amtfiid3 = dfDat.groupby(['rid','sic'])['amtfiid3'].sum()
	nfiid4 = dfDat.groupby(['rid','sic'])['nfiid4'].sum()
	amtfiid4 = dfDat.groupby(['rid','sic'])['amtfiid4'].sum()
	nfiid5 = dfDat.groupby(['rid','sic'])['nfiid5'].sum()
	amtfiid5 = dfDat.groupby(['rid','sic'])['amtfiid5'].sum()
	nfiid6 = dfDat.groupby(['rid','sic'])['nfiid6'].sum()
	amtfiid6 = dfDat.groupby(['rid','sic'])['amtfiid6'].sum()


	
	# find the max revenue from daily max revenue
	# and find daily slope from data with max (max revenue)
	
	maxRevDT = dfDat.groupby(['rid','sic', 'dt', 'slopeInfo', 'nWeek')['amtRev'].max()
	dfMaxRevDT = pd.DataFrame({'rid':[maxRevDT.index[i][0] for i in range(0,len(maxRevDT))],'sic':[maxRevDT.index[i][1] for i in range(0,len(maxRevDT))], 'dt':[maxRevDT.index[i][2] for i in range(0,len(maxRevDT))], 'slopeInfo':[maxRevDT.index[i][3] for i in range(0,len(maxRevDT))],  'nWeek':[maxRevDT.index[i][4] for i in range(0,len(maxRevDT))], 'maxRev':[maxRevDT[j] for j in range(0,len(maxRevDT))]})
	
	maxRev = dfMaxRevDT.groupby(['rid','sic'])['maxRev'].max()
	dfMaxRev = pd.DataFrame({'rid':[maxRev.index[i][0] for i in range(0,len(maxRev))],'sic':[maxRev.index[i][1] for i in range(0,len(maxRev))],'maxRev':[maxRev[j] for j in range(0,len(maxRev))]})
	dfMergeMaxRev = pd.merge(dfMaxRevDT,dfMaxRev,how='inner',on=['rid','sic','maxRev'])

	
	# merge all grouped data frame 

	df_ntc10 = pd.DataFrame({'rid':[ntc10.index[i][0] for i in range(0,len(ntc10))],'sic':[ntc10.index[i][1] for i in range(0,len(ntc10))], 'ntc10':[ntc10[j] for j in range(0,len(ntc10))]})
	df_amttc10 = pd.DataFrame({'rid':[amttc10.index[i][0] for i in range(0,len(amttc10))],'sic':[amttc10.index[i][1] for i in range(0,len(amttc10))], 'amttc10':[amttc10[j] for j in range(0,len(amttc10))]})
	df_ntc13 = pd.DataFrame({'rid':[ntc13.index[i][0] for i in range(0,len(ntc13))],'sic':[ntc13.index[i][1] for i in range(0,len(ntc13))], 'ntc13':[ntc13[j] for j in range(0,len(ntc13))]})
	df_amttc13 = pd.DataFrame({'rid':[amttc13.index[i][0] for i in range(0,len(amttc13))],'sic':[amttc13.index[i][1] for i in range(0,len(amttc13))], 'amttc13':[amttc13[j] for j in range(0,len(amttc13))]})
	df_ncashb = pd.DataFrame({'rid':[ncashb.index[i][0] for i in range(0,len(ncashb))],'sic':[ncashb.index[i][1] for i in range(0,len(ncashb))], 'ncashb':[ncashb[j] for j in range(0,len(ncashb))]})
	df_amtcashb = pd.DataFrame({'rid':[amtcashb.index[i][0] for i in range(0,len(amtcashb))],'sic':[amtcashb.index[i][1] for i in range(0,len(amtcashb))], 'amtcashb':[amtcashb[j] for j in range(0,len(amtcashb))]})
	df_nreturn = pd.DataFrame({'rid':[nreturn.index[i][0] for i in range(0,len(nreturn))],'sic':[nreturn.index[i][1] for i in range(0,len(nreturn))], 'nreturn':[nreturn[j] for j in range(0,len(nreturn))]})
	df_amtreturn = pd.DataFrame({'rid':[amtreturn.index[i][0] for i in range(0,len(amtreturn))],'sic':[amtreturn.index[i][1] for i in range(0,len(amtreturn))], 'amtreturn':[amtreturn[j] for j in range(0,len(amtreturn))]})
	df_ntc17 = pd.DataFrame({'rid':[ntc17.index[i][0] for i in range(0,len(ntc17))],'sic':[ntc17.index[i][1] for i in range(0,len(ntc17))], 'ntc17':[ntc17[j] for j in range(0,len(ntc17))]})
	df_amtrev = pd.DataFrame({'rid':[amtRev.index[i][0] for i in range(0,len(amtRev))],'sic':[amtRev.index[i][1] for i in range(0,len(amtRev))], 'amtRev':[amtRev[j] for j in range(0,len(amtRev))]})
	df_ntrans = pd.DataFrame({'rid':[ntrans.index[i][0] for i in range(0,len(ntrans))],'sic':[ntrans.index[i][1] for i in range(0,len(ntrans))], 'ntrans':[ntrans[j] for j in range(0,len(ntrans))]})

	df_nfiid1 = pd.DataFrame({'rid':[nfiid1.index[i][0] for i in range(0,len(nfiid1))],'sic':[nfiid1.index[i][1] for i in range(0,len(nfiid1))], 'nfiid1':[nfiid1[j] for j in range(0,len(nfiid1))]})
	df_amtfiid1 = pd.DataFrame({'rid':[amtfiid1.index[i][0] for i in range(0,len(amtfiid1))],'sic':[amtfiid1.index[i][1] for i in range(0,len(amtfiid1))], 'amtfiid1':[amtfiid1[j] for j in range(0,len(amtfiid1))]})
	df_nfiid2 = pd.DataFrame({'rid':[nfiid2.index[i][0] for i in range(0,len(nfiid2))],'sic':[nfiid2.index[i][1] for i in range(0,len(nfiid2))], 'nfiid2':[nfiid2[j] for j in range(0,len(nfiid2))]})
	df_amtfiid2 = pd.DataFrame({'rid':[amtfiid2.index[i][0] for i in range(0,len(amtfiid2))],'sic':[amtfiid2.index[i][1] for i in range(0,len(amtfiid2))], 'amtfiid2':[amtfiid2[j] for j in range(0,len(amtfiid2))]})
	df_nfiid3 = pd.DataFrame({'rid':[nfiid3.index[i][0] for i in range(0,len(nfiid3))],'sic':[nfiid3.index[i][1] for i in range(0,len(nfiid3))], 'nfiid3':[nfiid3[j] for j in range(0,len(nfiid3))]})
	df_amtfiid3 = pd.DataFrame({'rid':[amtfiid3.index[i][0] for i in range(0,len(amtfiid3))],'sic':[amtfiid3.index[i][1] for i in range(0,len(amtfiid3))], 'amtfiid3':[amtfiid3[j] for j in range(0,len(amtfiid3))]})
	df_nfiid4 = pd.DataFrame({'rid':[nfiid4.index[i][0] for i in range(0,len(nfiid4))],'sic':[nfiid4.index[i][1] for i in range(0,len(nfiid4))], 'nfiid4':[nfiid4[j] for j in range(0,len(nfiid4))]})
	df_amtfiid4 = pd.DataFrame({'rid':[amtfiid4.index[i][0] for i in range(0,len(amtfiid4))],'sic':[amtfiid4.index[i][1] for i in range(0,len(amtfiid4))], 'amtfiid4':[amtfiid4[j] for j in range(0,len(amtfiid4))]})
	df_nfiid5 = pd.DataFrame({'rid':[nfiid5.index[i][0] for i in range(0,len(nfiid5))],'sic':[nfiid5.index[i][1] for i in range(0,len(nfiid5))], 'nfiid5':[nfiid5[j] for j in range(0,len(nfiid5))]})
	df_amtfiid5 = pd.DataFrame({'rid':[amtfiid5.index[i][0] for i in range(0,len(amtfiid5))],'sic':[amtfiid5.index[i][1] for i in range(0,len(amtfiid5))], 'amtfiid5':[amtfiid5[j] for j in range(0,len(amtfiid5))]})
	df_nfiid6 = pd.DataFrame({'rid':[nfiid6.index[i][0] for i in range(0,len(nfiid6))],'sic':[nfiid6.index[i][1] for i in range(0,len(nfiid6))], 'nfiid6':[nfiid6[j] for j in range(0,len(nfiid6))]})
	df_amtfiid6 = pd.DataFrame({'rid':[amtfiid6.index[i][0] for i in range(0,len(amtfiid6))],'sic':[amtfiid6.index[i][1] for i in range(0,len(amtfiid6))], 'amtfiid6':[amtfiid6[j] for j in range(0,len(amtfiid6))]})

	df1 = pd.merge(df_ntc10,df_amttc10,how='outer',on=['rid','sic'])
	df2 = pd.merge(df_ntc13,df_amttc13,how='outer',on=['rid','sic'])
	df3 = pd.merge(df_ncashb,df_amtcashb,how='outer',on=['rid','sic'])
	df4 = pd.merge(df_nreturn,df_amtreturn,how='outer',on=['rid','sic'])
	df5 = pd.merge(df1,df2,how='outer',on=['rid','sic'])
	df6 = pd.merge(df5,df3,how='outer',on=['rid','sic'])
	df7 = pd.merge(df6,df4,how='outer',on=['rid','sic'])
	df8 = pd.merge(df7,df_ntc17,how='outer',on=['rid','sic'])

	df9 = pd.merge(df_nfiid1,df_amtfiid1,how='outer',on=['rid','sic'])
	df10 = pd.merge(df_nfiid2,df_amtfiid2,how='outer',on=['rid','sic'])
	df11 = pd.merge(df_nfiid3,df_amtfiid3,how='outer',on=['rid','sic'])
	df12 = pd.merge(df_nfiid4,df_amtfiid4,how='outer',on=['rid','sic'])
	df13 = pd.merge(df_nfiid5,df_amtfiid5,how='outer',on=['rid','sic'])
	df14 = pd.merge(df_nfiid6,df_amtfiid6,how='outer',on=['rid','sic'])
	df15 = pd.merge(df9,df10,how='outer',on=['rid','sic'])
	df16 = pd.merge(df15,df11,how='outer',on=['rid','sic'])
	df17 = pd.merge(df16,df12,how='outer',on=['rid','sic'])
	df18 = pd.merge(df17,df13,how='outer',on=['rid','sic'])
	df19 = pd.merge(df18,df14,how='outer',on=['rid','sic'])
	
	df20 = pd.merge(df8,df19,how='outer',on=['rid','sic'])
	df21 = pd.merge(df20,df_amtrev,how='outer',on=['rid','sic'])
	df22 = pd.merge(df21,df_ntrans,how='outer',on=['rid','sic'])
	groupedDat = pd.merge(df22,dfMergeMaxRev,how='outer',on=['rid','sic'])

	groupedDat['ntc10']=groupedDat['ntc10'].fillna(0)
	groupedDat['amttc10']=groupedDat['amttc10'].fillna(0)
	groupedDat['ntc13']=groupedDat['ntc13'].fillna(0)
	groupedDat['amttc13']=groupedDat['amttc13'].fillna(0)
	groupedDat['ncashb']=groupedDat['ncashb'].fillna(0)
	groupedDat['amtcashb']=groupedDat['amtcashb'].fillna(0)
	groupedDat['nreturn']=groupedDat['nreturn'].fillna(0)
	groupedDat['amtreturn']=groupedDat['amtreturn'].fillna(0)
	groupedDat['ntc17']=groupedDat['ntc17'].fillna(0)
	
	groupedDat['nfiid1']=groupedDat['nfiid1'].fillna(0)
	groupedDat['nfiid2']=groupedDat['nfiid2'].fillna(0)
	groupedDat['nfiid3']=groupedDat['nfiid3'].fillna(0)
	dataprep['nfiid4']=groupedDat['nfiid4'].fillna(0)
	groupedDat['nfiid5']=groupedDat['nfiid5'].fillna(0)
	groupedDat['nfiid6']=groupedDat['nfiid6'].fillna(0)
	groupedDat['amtfiid1']=groupedDat['amtfiid1'].fillna(0)
	groupedDat['amtfiid2']=groupedDat['amtfiid2'].fillna(0)
	groupedDat['amtfiid3']=groupedDat['amtfiid3'].fillna(0)
	groupedDat['amtfiid4']=groupedDat['amtfiid4'].fillna(0)
	groupedDat['amtfiid5']=groupedDat['amtfiid5'].fillna(0)
	groupedDat['amtfiid6']=groupedDat['amtfiid6'].fillna(0)

	groupedDat['amtRev']=groupedDat['amtRev'].fillna(0)
	groupedDat['ntrans']=groupedDat['ntrans'].fillna(0)
	

	return groupedDat

def getModeTime(dfDat):
	
	# find the most frequent time of day and datetime when max revenue occurs
	datTimeOfDay = dfDat[['rid', 'sic', 'timeOfDay']]
	# mode of timeOfDay for every rid+sic key
	grpTimeOfDay = datTimeOfDay.groupby(['rid','sic'])
	arrName=[]
	tmpArr=[]
	modeTime = 0
	dfModeTime = pd.DataFrame({'rid':[],'sic':[], 'timeMaxRev':[]})
	for name, group in grpTimeOfDay:
		arrName.append(name)
	for j in range(len(arrName)):
		tmpArr = grp.get_group(arrName[j])
		tmpArr = tmpArr.set_index(['rid', 'sic'])
		modeTmp = mode(tmpArr['timeOfDay'])
		modeTime = modeTmp[0][0]
		dfTimeOfDay = pd.DataFrame({'rid':(tmpArr.index[0][0]),'sic':(tmpArr.index[0][1]), 'timeMaxRev':(modeTime)})
		dfModeTime = dfModeTime.append(dfTimeOfDay, ignore_index=True)

	return dfModeTime

def getModeWeek(dfDat):
	
	# find the most frequent week when max revenue occurs : 1 = week 1; 2 = week 2
	dfDat['week'] = np.where((dfDat['nWeek']%2==0),2,1)
	datWeek = dfDat[['rid', 'sic', 'week']]
	# mode of timeOfDay for every rid+sic key
	grpWeekRev = datWeek.groupby(['rid','sic'])
	arrName=[]
	tmpArr=[]
	modeWeek = 0
	dfModeWeek = pd.DataFrame({'rid':[],'sic':[], 'weekMaxRev':[]})
	for name, group in grpWeekRev:
		arrName.append(name)
	for j in range(len(arrName)):
		tmpArr = grpWeekRev.get_group(arrName[j])
		tmpArr = tmpArr.set_index(['rid', 'sic'])
		modeTmp = mode(tmpArr['week'])
		modeWeek = modeTmp[0][0]
		dfWeekTmp = pd.DataFrame({'rid':(tmpArr.index[0][0]),'sic':(tmpArr.index[0][1]), 'weekMaxRev':(modeWeek)})
		dfModeWeek = dfModeWeek.append(dfWeek, ignore_index=True)

	return dfModeWeek

def getCont(dfDat):

	# get initial contribution (only first fortnightly revenue of training data)
	dfRev = dfDat[['rid','sic', 'amtRev', 'nWeek']]
	# initial contribution is the sum of fortnight revenue in the first two week of observed data
	amtRev = dfRev.groupby(['rid','sic'])['amtRev'].sum()
	dfInitCont = pd.DataFrame({'rid':[amtRev.index[i][0] for i in range(0,len(amtRev))],'sic':[amtRev.index[i][1] for i in range(0,len(amtRev))], 'initCont':[amtRev[j] for j in range(0,len(amtRev))]})

	# get first week and second week contribution within first fortnight data
	dfGrp = pd.DataFrame({'rid':[],'sic':[], 'contRatio':[]})
	revGrp = dfRev.groupby(['rid','sic'])
	arrName=[]
	tmpArr=[]
	fCont = 0
	lCont = 0
	contRatio = 0
	for name, group in revGrp:
		arrName.append(name)
	for j in range(len(arrName)):
		tmpArr = revGrp.get_group(arrName[j])
		tmpArr = tmpArr.sort_index(by='nWeek', ascending=True)
		tmpArr = tmpArr.set_index(['rid','sic'])
		# get value of fCont and lCont from first and second week of revenue

		if len(tmpArr)==2:
			fCont = tmpArr['amtRev'][0]
			lCont = tmpArr['amtRev'][1]
			tmpCont = fCont/lCont

			if tmpCont > 1:
				contRatio = 3
			else:
				contRatio = 4

			dfTmp = pd.DataFrame({'rid':(tmpArr.index[0][0]),'sic':(tmpArr.index[0][1]), 'contRatio':(contRatio)})
			dfGrp = dfGrp.append(dfTmp, ignore_index=True)
		# if retailer data (rid, sic) only appears in one week
		else:
			if tmpArr['nWeek'][0]==1:
				fCont = tmpArr['amtRev'][0]
				lCont = 0
				contRatio = 1
			else:
				fCont = 0
				lCont = tmpArr['amtRev'][0]
				contRatio = 2
			dfTmp = pd.DataFrame({'rid':(tmpArr.index[0][0]),'sic':(tmpArr.index[0][1]), 'contRatio':(contRatio)})
			dfGrp = dfGrp.append(dfTmp, ignore_index=True)
	
	# merge two dataframes
	dfCont = pd.merge(dfInitCont,dfGrp,how='inner',on=['rid','sic'])

	return dfCont

def getSlope(dfDat, opt):

	
	# opt 1 = weekly slope (only consists of slopeInfo information and nWeek to generate changePoint attribute); opt 2 = fortnightly slope;opt 3 = all data slope (consists of all information: slope, intercept, gradient, slopeInfo)
	
	# getting the function of slope (linear regression with gradient descent)
	dat = dfDat[['rid','sic','dt', 'nWeek', 'amtRev']]
	datSorted = dat.sort_index(by='dt', ascending = True)
	datGrp=datSorted.groupby(['rid','sic'])
	slopeDat = pd.DataFrame({'rid':[],'sic':[],'nWeek':[], 'intercept':[], 'slope':[]})
	arrName=[]
	tmpArr=[]
	theta=[]
	nWeek = 0

	for name, group in datGrp:
		arrName.append(name)
    for j in range(len(arrName)):
		tmpArr = datGrp.get_group(arrName[j])
		tmpArr = tmpArr.set_index(['rid','sic'])
		if len(tmpArr)>1:
			dfTmp = pd.DataFrame({'rid':[tmpArr.index[i][0] for i in range(0,len(tmpArr))],'sic':[tmpArr.index[i][1] for i in range(0,len(tmpArr))], 'dt':[tmpArr['dt'][i] for i in range(0,len(tmpArr))], 'nWeek':[tmpArr['nWeek'][i] for i in range(0,len(tmpArr))], 'amtRev':[tmpArr['amtRev'][i] for i in range(0,len(tmpArr))]})
			dfTmpSorted = dfTmp.sort_index(by='dt', ascending=True)
			dfTmpSorted['sorted']=[i for i in range(0,len(dfTmpSorted))]
			
			if opt == 1:
				nWeek = dfTmpSorted['nWeek'][0]
			else:
				nWeek = dfTmpSorted['nWeek'][len(dfTmpSorted)]
		
				
  			theta = linReg(dfTmpSorted['sorted'], dfTmpSorted['amtRev'])
			dfTheta = pd.DataFrame({'rid':(dfTmpSorted['rid'][0]),'sic':dfTmpSorted['sic'][0], 'nWeek': nWeek, 'intercept':theta[0], 'slope':theta[1]})
			slopeDat = slopeDat.append(dfTheta, ignore_index=True)
		else:
			continue

	slopeDat['gradient']=slopeDat['slope'].apply(lambda x:round(math.degrees(np.arctan(x)),2))
	# slope positive = 1, slope negative = 2 
	slopeDat['slopeInfo']=slopeDat['slope'].apply(lambda x: 1 if x > 0 else 2)

	if (opt == 1) & (opt == 2):
		slopeData = slopeDat[['rid', 'sic', 'nWeek', 'slopeInfo']]
	else:
		slopeData = slopeDat[['rid', 'sic', 'intercept', 'slope', 'slopeInfo', 'gradient']]

	return slopeData



def getChangePoint(dfFortnight):

	
	#change point
	changePoint = 0 
	nWeek = 0
	dfFortnight = dfFortnight.sort_index(by='nWeek', ascending = True)
	dfChangePoint = pd.DataFrame({'rid':[],'sic':[],'changePoint':[], 'nWeek':[]})
	
	if len(dfFortnight)==2:
		nWeek = dfFortnight['nWeek'][1]
		# rise - fall
		if ((dfFortnight['slopeInfo'][0] == 1) & (dfFortnight['slopeInfo'][1] == 2)):
			changePoint = 1
		# fall - rise
		elif ((dfFortnight['slopeInfo'][0] == 2) & (dfFortnight['slopeInfo'][1] == 1)):
			changePoint = 2
		# rise - rise
		elif ((dfFortnight['slopeInfo'][0] == 1) & (dfFortnight['slopeInfo'][1] == 1)):
			changePoint = 3
		# fall - fall 
		elif ((dfFortnight['slopeInfo'][0] == 2) & (dfFortnight['slopeInfo'][1] == 2)):
			changePoint = 4

	# if retailer data only appears once within fortnight data
	else:
		nWeek = dfFortnight['nWeek'][0]
		# if it only appears in week 1
		# rise - flat
		if ((dfFortnight['nWeek'][0] == 1) & (dfFortnight['slopeInfo'][1] == 1)):
			changePoint = 5
		# fall - flat
		elif ((dfFortnight['nWeek'][0] == 1) & (dfFortnight['slopeInfo'][1] == 2)):
			changePoint = 6
		
		# if it only appears in week 2
		# flat - rise
		elif ((dfFortnight['nWeek'][0] == 2) & (dfFortnight['slopeInfo'][1] == 1)):
			changePoint = 7
		# flat - fall
		elif ((dfFortnight['nWeek'][0] == 2) & (dfFortnight['slopeInfo'][1] == 2)):
			changePoint = 8
			
	df = pd.DataFrame({'rid':(dfFortnight['rid'][0]),'sic':(dfFortnight['sic'][0]), 'changePoint': changePoint, 'nWeek': nWeek})
	dfChangePoint = dfChangePoint.append(df, ignore_index = True)


	return dfChangePoint

def combineChangePoint(df2WChangePoint, df2WSlope):

	# combination of two weeks slope and change points --> to generate mode of combination
	dfMrg = pd.merge(df2WChangePoint,df2WSlope, on=['rid','sic','nWeek'])
	dfMrg['combineSlope']=np.where((dfMrg['slopeInfo'] == '1'),(dfMrg['changePoint']),(dfMrg['changePoint']+8)) 

	tmp = dfMrg[['rid', 'sic', 'combineSlope']]
	# mode of combination
	dfCombine = tmp.groupby(['rid','sic'])
	arrName=[]
	tmpArr=[]
	modeComb = 0
	dfModeCP = pd.DataFrame({'rid':[],'sic':[], 'combineSlope':[]})
	for name, group in dfCombine:
		arrName.append(name)
	for j in range(len(arrName)):
		tmpArr = dfCombine.get_group(arrName[j])
		tmpArr = tmpArr.set_index(['rid', 'sic'])
		modeTmp = mode(tmpArr['combineSlope'])
		modeCP = modeTmp[0][0]
		dfCP = pd.DataFrame({'rid':(tmpArr.index[0][0]),'sic':(tmpArr.index[0][1]), 'combineSlope':(modeCP)})
		dfModeCP = dfModeCP.append(dfCP, ignore_index=True)

	return dfModeCP


def mergeDat(dfList):

	
	# temporary variables
	 dfWeek = pd.DataFrame({'rid':[],'sic':[],'ntc10':[],'amttc10':[],'ntc13':[],'amttc13':[],'ncashb':[],'amtcashb':[], 'nreturn':[], 'amtreturn':[], 'ntc17':[],'nfiid1':[],'amtfiid1':[],'nfiid2':[],'amtfiid2':[],'nfiid3':[], 'amtfiid3':[], 'nfiid4':[],'amtfiid4':[],'nfiid5':[],'amtfiid5':[],'nfiid6':[], 'amtfiid6':[], 'amtRev':[], 'ntrans':[], 'maxRev':[], 'timeOfDay':[], 'dt':[], 'slopeInfo':[]})
	dfFortnight = pd.DataFrame({'rid':[],'sic':[],'ntc10':[],'amttc10':[],'ntc13':[],'amttc13':[],'ncashb':[],'amtcashb':[], 'nreturn':[], 'amtreturn':[], 'ntc17':[],'nfiid1':[],'amtfiid1':[],'nfiid2':[],'amtfiid2':[],'nfiid3':[], 'amtfiid3':[], 'nfiid4':[],'amtfiid4':[],'nfiid5':[],'amtfiid5':[],'nfiid6':[], 'amtfiid6':[], 'amtRev':[], 'ntrans':[], 'maxRev':[], 'timeOfDay':[], 'dt':[], 'slopeInfo':[], 'nWeek':[]})
	df1WSlope = pd.DataFrame({'rid':[],'sic':[],'slopeInfo':[]})
	df2WCP = pd.DataFrame({'rid':[],'sic':[],'slopeInfo':[]})

	dfAll2W = pd.DataFrame({'rid':[],'sic':[],'ntc10':[],'amttc10':[],'ntc13':[],'amttc13':[],'ncashb':[],'amtcashb':[], 'nreturn':[], 'amtreturn':[], 'ntc17':[],'nfiid1':[],'amtfiid1':[],'nfiid2':[],'amtfiid2':[],'nfiid3':[], 'amtfiid3':[], 'nfiid4':[],'amtfiid4':[],'nfiid5':[],'amtfiid5':[],'nfiid6':[], 'amtfiid6':[], 'amtRev':[], 'ntrans':[], 'maxRev':[], 'timeMaxRev':[], 'dt':[], 'slopeInfo':[], 'nWeek':[]})

	dfAll = pd.DataFrame({'rid':[],'sic':[],'ntc10':[],'amttc10':[],'ntc13':[],'amttc13':[],'ncashb':[],'amtcashb':[], 'nreturn':[], 'amtreturn':[], 'ntc17':[],'nfiid1':[],'amtfiid1':[],'nfiid2':[],'amtfiid2':[],'nfiid3':[], 'amtfiid3':[], 'nfiid4':[],'amtfiid4':[],'nfiid5':[],'amtfiid5':[],'nfiid6':[], 'amtfiid6':[], 'amtRev':[], 'ntrans':[], 'maxRev':[], 'timeMaxRev':[], 'dt':[], 'slopeInfo':[], 'intercept':[], 'slope':[],'gradient':[], 'slopeInfo':[], 'nWeek':[]})
	
	dfCont = pd.DataFrame({'rid':[],'sic':[],'initCont':[], 'contRatio':[]})

	df2WSlope = pd.DataFrame({'rid':[],'sic':[],'slopeInfo':[]})

	df2WChangePoint = pd.DataFrame({'rid':[],'sic':[],'slopeInfo':[]})
	
	
	# 42 datafiles
	num = range(0, len(dfList))
    startOfWeek = [0, 7, 14, 21, 28, 35]
    j = 0
	for i in num:
		if i in startOfWeek:
			j = j + 1
			nStart = i
			nEnd = int(i) + 7
			nRange = range(nStart, nEnd)

			# merge data every week
			for n in nRange:

				# skip empty dataframe
				if n == 17:
					continue
				
				dfWeek=dfWeek.append(dfList[n], ignore_index = True)
				dfWeek['nWeek'] = j
				
				# store weekly data to generate 6w slope later
				dfAll = dfAll.append(dfWeek, ignore_index = True)
			
			'''
			attributes created from grouping daily data into weekly data:
				rid, sic, ntc10, amttc10, ntc13, amttc13, ncashb, amtcashb, nreturn, amtreturn, ntc17, , nfiid1, amtfiid1, nfiid2, amtfiid2, nfiid3, amtfiid3, nfiid4, amtfiid4, nfiid5, amtfiid5, nfiid6, amtfiid6, amtRev, ntrans, maxRev, dt (dtMaxRev), slopeInfo (maxDailySlope), nWeek (weekMaxRev)
			'''
			# get one week summarized data
			dfOneWeek = groupingData(dfWeek)

			# get one week slope from daily data
			# resulting dataframe with attributes: rid, sic, slopeInfo, nWeek
			df1WSlope = getSlope(dfWeek, 1)
			df2WCP = df2WCP.append(df1WSlope, ignore_index = True)

			# append weekly summarised data into fortnightly
			dfFortnight = dfFortnight.append(dfOneWeek, ignore_index = True)

			# clear dataframe of appended and summarized weekly data (temporary variable)
			dfWeek = dfWeek.drop(dfWeek.index[:len(dfWeek)])
			dfOneWeek = None
			
			# merge data every two weeks
			if j % 2 == 0:

				

				# get changePoint attribute
				dfCP = getChangePoint(df2WCP)
				df2WChangePoint = df2WChangePoint.append(dfCP, ignore_index = True)

				# empty variables and data frame
				df1WSlope = None
				df2WCP = df2WCP.drop(df2WCP.index[:len(df2WCP)])
				dfCP = None
				
				
				# summarized data from fortnightly data
				'''
				attributes created from grouping daily data into weekly data:
				rid, sic, ntc10, amttc10, ntc13, amttc13, ncashb, amtcashb, nreturn, amtreturn, ntc17, , nfiid1, amtfiid1, nfiid2, amtfiid2, nfiid3, amtfiid3, nfiid4, amtfiid4, nfiid5, amtfiid5, nfiid6, amtfiid6, amtRev, ntrans, maxRev, dt (dtMaxRev), slopeInfo (maxDailySlope), nWeek (weekMaxRev)
				'''
				dat2W = groupingData(dfFortnight)

				# store summarized fortnightly data for all 6 weeks data
				dfAll2W = dfAll2W.append(dat2W, ignore_index = True)

				# get 2 W slope from daily data within 2 weeks
				# attributes generated: 
				dfSlope2W = getSlope(dfAll, 2)
				df2WSlope = df2WSlope.append(dfSlope2W, ignore_index=True)

				# attribute initial contribution is computed from first two week data
				if j == 2:
				
					
					# get attribute initCont, contRatio
					# this attribute later is merged with all aggregated data within 6 w
					# resulting dataframe with attributes: rid, sic, initCont, contRatio
					df2WCont = getCont(dfFortnight)
					dfCont = dfCont.append(df2WCont, ignore_index = True)
				
				
				# empty dataframe for fortnightly data (temporary variable) 
				dfFortnight = dfFortnight.drop(dfFortnight.index[:len(dfFortnight)])
				dat2W = None
				dfSlope2W = None
				df2WCont = None
			else:
				continue
		else:
			continue

	
	# combine 2W slope and change point then retrieve the mode: rid, sic, combineSlope
	dfCombineSlope = combineChangePoint(df2WChangePoint, df2WSlope)
		
	'''
	merging all summary data from aggregated fortnightly data
	attributes created:
		rid, sic, ntc10, amttc10, ntc13, amttc13, ncashb, amtcashb, nreturn, amtreturn, ntc17, , nfiid1, amtfiid1, nfiid2, amtfiid2, nfiid3, amtfiid3, nfiid4, amtfiid4, nfiid5, amtfiid5, nfiid6, amtfiid6, amtRev, ntrans, maxRev, dt, slopeInfo, nWeek

	'''
	dfSum6W = groupingData(dfAll2W)
	dfSum6W['maxDailySlope']=dfSum6W['slopeInfo']
	dfSum6W = dfSum6W.drop('slopeInfo',1)
	dfSum6W = dfSum6W.drop('dt',1)

	# calculate mean revenue
	dfSum6W['meanRev'] = ((dfSum6W['amtRev'])/(dfSum6W['ntrans']))
	# normalised mean and max revenue
	
	minMeanRev = min(dfSum6W['meanRev'])
	maxMeanRev = max(dfSum6W['meanRev'])
	minMaxRev = min(dfSum6W['maxRev'])
	maxMaxRev = max(dfSum6W['maxRev'])

	dfSum6W['normMeanRev'] = dfSum6W['meanRev'].apply(lambda x:((x-minMeanRev)/(maxMeanRev-minMeanRev)))
	dfSum6W['normMaxRev'] = dfSum6W['maxRev'].apply(lambda x:((x-minMaxRev)/(maxMaxRev-minMaxRev)))
	dfSum6W = dfSum6W.drop('ntrans',1)
	dfSum6W = dfSum6W.drop('nWeek',1)
	dfSum6W = dfSum6W.drop('amtRev',1)
	dfSum6W = dfSum6W.drop('meanRev',1)
	dfSum6W = dfSum6W.drop('maxRev',1)

	# get mode of week when max revenue occurs : rid, sic, weekMaxRev
	dfWeekMaxRev = getModeWeek(dfAll2W)
	
	

	''' 
	merging all daily data:
		- slope of daily amtRev: rid, sic, slope, intercept, slopeInfo, gradient
		- mode of daily timeOfDay when max revenue occurs: rid, sic, timeMaxRev
	'''
	dfSlope6W = getSlope(dfAll, 3)
	dfDayTime6W = getModeTime(dfAll)

	
	
	# merge all 
	'''
	rid, sic, initCont, contRatio, combineSlope 
	'''
	df1 = pd.merge(dfCont,dfCombineSlope,how='inner',on=['rid','sic'])
	'''
	rid, sic, ntc10, amttc10, ntc13, amttc13, ncashb, amtcashb, nreturn, amtreturn, ntc17, , nfiid1, amtfiid1, nfiid2, amtfiid2, nfiid3, amtfiid3, nfiid4, amtfiid4, nfiid5, amtfiid5, nfiid6, amtfiid6, maxDailySlope, normMeanRev, normMaxRev, , initCont, contRatio, combineSlope
	'''
	df2 = pd.merge(dfSum6W,df1,how='inner',on=['rid','sic'])
	'''
	rid, sic, ntc10, amttc10, ntc13, amttc13, ncashb, amtcashb, nreturn, amtreturn, ntc17, , nfiid1, amtfiid1, nfiid2, amtfiid2, nfiid3, amtfiid3, nfiid4, amtfiid4, nfiid5, amtfiid5, nfiid6, amtfiid6, maxDailySlope, normMeanRev, normMaxRev, initCont, contRatio, combineSlope, slope, intercept, slopeInfo, gradient
	'''
	df3 = pd.merge(dfSlope6W,df2,how='inner',on=['rid','sic'])
	'''
	rid, sic, ntc10, amttc10, ntc13, amttc13, ncashb, amtcashb, nreturn, amtreturn, ntc17, , nfiid1, amtfiid1, nfiid2, amtfiid2, nfiid3, amtfiid3, nfiid4, amtfiid4, nfiid5, amtfiid5, nfiid6, amtfiid6, maxDailySlope, normMeanRev, normMaxRev, initCont, contRatio, combineSlope, slope, intercept, slopeInfo, gradient, timeMaxRev
	'''
	df4 = pd.merge(dfDayTime6W,df3,how='inner',on=['rid','sic'])
	'''
	rid, sic, ntc10, amttc10, ntc13, amttc13, ncashb, amtcashb, nreturn, amtreturn, ntc17, , nfiid1, amtfiid1, nfiid2, amtfiid2, nfiid3, amtfiid3, nfiid4, amtfiid4, nfiid5, amtfiid5, nfiid6, amtfiid6, maxDailySlope, normMeanRev, normMaxRev, initCont, contRatio, combineSlope, slope, intercept, slopeInfo, gradient, timeMaxRev, weekMaxRev
	'''
	dataAll = pd.merge(dfWeekMaxRev,df4,how='inner',on=['rid','sic'])
	
	return dataAll





prep17=[]
dfList = [prep1, prep2, prep3, prep4, prep5, prep6, prep7, prep8, prep9, prep10, prep11, prep12, prep13, prep14, prep15, prep16, prep17, prep18, prep19, prep20, prep21, prep22, prep23, prep24, prep25, prep26, prep27, prep28, prep29, prep30, prep31, prep32, prep33, prep34, prep35, prep36, prep37, prep38, prep39, prep40, prep41, prep42]

dataAll = mergeDat(dfList)
