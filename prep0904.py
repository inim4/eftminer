import pandas as pd
import numpy as np
import math
import time
import datetime
from datetime import datetime
from sklearn.linear_model import LinearRegression



def readData(fileDat):
	
	headstr=['strretailerid','str_time', 'str_sic','stracqfiid', 'strcardfiid','strcardno','strtrancde','famt1','famt2', 'tc','t','aa','c']
	dat = pd.read_table(fileDat,sep='\t',index_col=None,header=None,names=headstr,error_bad_lines=False,dtype = unicode); 
	alldata = dat[1:len(dat)]

	return alldata


def selectData(datparam, strdate):


	'''
	#parameters
	grouping by retailer ID + SIC
	prevalent SIC:
	- communication					48
	- wholesale trade				50
	- hardware / build materials	52
	- merchandise store				53
	- food store					54
	- automotive dealer & gasoline	55
	- apparels & accessories		56
	- furniture						57
	- eating & drinking				58
	- misc retail					59
	- insurances					63
	- hotels						70
	- beauty						72
	- business services				73
	- automotive repair				75
	- motion pictures				78
	'''
	datparam['strdate'] = strdate
	datparam['famt1'] = datparam['famt1'].astype(float)
	datparam['famt2'] = datparam['famt2'].astype(float)
	datparam['sic'] = [sic[:2] for sic in datparam['str_sic']]
	siclist=['48','50','52','53','54','55','56','57','58','59','63','70','72','73','75','78']
	cardlist=['1','2']
	dat = datparam[(datparam['sic'].isin(siclist)) & (datparam['t'].isin(cardlist))]
	dat['acqfiid']= np.where(dat['stracqfiid'].str.contains('ANZ'),'ANZ','NONANZ')
	dat['cardfiid']= np.where(dat['strcardfiid'].str.contains('ANZ'),'ANZ','NONANZ')
	
	
	
	return dat

def selectTrans(dat):

	'''
	convert / discretize datetime to time of a day
	morning (1) -- 00.01 - 12.00
	noon (2) -- 12.00 - 18.00
	evening (3) -- 18.00 - 24.00
	'''
	dat['str_time']=dat['str_time'].apply(lambda x:str(x).replace('.',''))
	dattmp = dat[dat['str_time'].str.len()==12]
	dattmp[dattmp[['str_time']].apply(lambda x: (str(x)).isdigit(), axis=1)]
	dattmp['hh']=[hhmmss[:2] for hhmmss in dattmp['str_time']]
	dattmp['mm']=[hhmmss[2:4] for hhmmss in dattmp['str_time']]
	dattmp['ss']=[hhmmss[4:6] for hhmmss in dattmp['str_time']]
	dattmp['ms']=[hhmmss[6:12] for hhmmss in dattmp['str_time']]
	dattmp['hhInt']=dattmp['hh'].apply(lambda x:int(x))
	dattmp['mmInt']=dattmp['mm'].apply(lambda x:int(x))
	dattmp['ssInt']=dattmp['ss'].apply(lambda x:int(x))
	dattmp['fms'] = dattmp['ms'].apply(lambda x: float(x)/1000000)
	datFilter = dattmp[(dattmp['hhInt'] < 24) & (dattmp['mmInt'] < 60) & (dattmp['ssInt'] < 60) & (dattmp['fms'] <= 1)]
	datFilter['mergeseconds']=datFilter['ssInt'] + datFilter['fms']
	datFilter['secstr'] = datFilter['mergeseconds'].apply(lambda x: '0'+'%.6f' % float(x) if x<10 else '%.6f' % float(x))
	datFilter['strdatetime'] = datFilter['strdate'] + ' ' + datFilter['hh'] + ':' + datFilter['mm'] + ':' + datFilter['secstr']
	datFilter['dt']=datFilter['strdatetime'].apply(lambda x:datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
	datFilter['is_morning']=datFilter['dt'].apply(lambda x: 1 if x.hour < 12 else 0)
	datFilter['is_noon']=datFilter['dt'].apply(lambda x: 2 if (x.hour > 12 and x.hour <=18)  else 0)
	datFilter['is_evening']=datFilter['dt'].apply(lambda x: 3 if (x.hour > 18 and x.hour <=24) else 0)
	datFilter['daytime'] = datFilter['is_morning'].apply(lambda x: int(x)) + datFilter['is_noon'].apply(lambda x: int(x)) + datFilter['is_evening'].apply(lambda x: int(x))

	cdepos=['10','12','13', '17', '18','21','24','62']
	cdemin=['14','22']
	rev = datFilter[datFilter['tc'].isin(cdepos)]
	retrn = datFilter[datFilter['tc'].isin(cdemin)]
	datTrans =[rev,retrn]

	return datTrans

def frameTrans(datTrans):

	rev = datTrans[0]
	retrn = datTrans[1]
	retrn['famt']=retrn['famt1']

	'''
	numtc10 (n total normal purchase transaction)
	sumtc10 (monetary amount of normal purchase transaction)
	'''
	tc10 = rev[rev['tc']=='10']
	tc10['famt']=np.where((tc10['c'] == '3'),tc10['famt2'],tc10['famt1'])
	numtc10 = tc10.groupby(['strretailerid','sic']).size()
	sumtc10 = tc10.groupby(['strretailerid','sic'])['famt'].sum()

	'''
	numtc13 (n total mail/online purchase transaction)
	sumtc13 (monetary amount of mail/online purchase transaction)
	'''
	tc13 = rev[rev['tc']=='13']
	tc13['famt']=np.where((tc13['c'] == '3'),tc13['famt2'],tc13['famt1'])
	numtc13 = tc13.groupby(['strretailerid','sic']).size()
	sumtc13 = tc13.groupby(['strretailerid','sic'])['famt'].sum()

	'''
	numcashb (n total cashback transaction)
	sumcashb (monetary amount of cashback transaction)
	'''
	tc_cashb = rev[(rev['tc']=='18')|(rev['tc']=='24')]
	tc_cashb['famt'] = np.where((tc_cashb['c'] == '3'),(tc_cashb['famt2']),(tc_cashb['famt1']-tc_cashb['famt2']))
	numtccashb = tc_cashb.groupby(['strretailerid','sic']).size()
	sumtccashb = tc_cashb.groupby(['strretailerid','sic'])['famt'].sum()

	'''
	n total balance inquiry transaction
	'''
	tc17 = rev[rev['tc']=='17']
	numtc17 = tc17.groupby(['strretailerid','sic']).size()
	tc17['famt'] = 0

	'''
	other transactions
	'''
	tc_auth = rev[(rev['tc']=='12')|(rev['tc']=='62')]
	tc_auth['famt']=np.where((tc_auth['c'] == '3'),tc_auth['famt2'],tc_auth['famt1'])
	tc21= rev[rev['tc']=='21']
	tc21['famt']=tc21['famt2']


	'''
	merging transaction
	'''	
	transArr = [tc10,tc13,tc_cashb,tc17,tc_auth,tc21]
	transMrg = pd.concat(transArr, ignore_index=True)


	df_ntc10 = pd.DataFrame({'rid':[numtc10.index[i][0] for i in range(0,len(numtc10))],'sic':[numtc10.index[i][1] for i in range(0,len(numtc10))],'ntc10':[numtc10[j] for j in range(0,len(numtc10))]})
	df_amttc10 = pd.DataFrame({'rid':[sumtc10.index[i][0] for i in range(0,len(sumtc10))],'sic':[sumtc10.index[i][1] for i in range(0,len(sumtc10))],'amttc10':[sumtc10[j] for j in range(0,len(sumtc10))]})
	df_ntc13 = pd.DataFrame({'rid':[numtc13.index[i][0] for i in range(0,len(numtc13))],'sic':[numtc13.index[i][1] for i in range(0,len(numtc13))],'ntc13':[numtc13[j] for j in range(0,len(numtc13))]})
	df_amttc13 = pd.DataFrame({'rid':[sumtc13.index[i][0] for i in range(0,len(sumtc13))],'sic':[sumtc13.index[i][1] for i in range(0,len(sumtc13))],'amttc13':[sumtc13[j] for j in range(0,len(sumtc13))]})
	df_ncashb = pd.DataFrame({'rid':[numtccashb.index[i][0] for i in range(0,len(numtccashb))],'sic':[numtccashb.index[i][1] for i in range(0,len(numtccashb))],'ncashb':[numtccashb[j] for j in range(0,len(numtccashb))]})
	df_amtcashb = pd.DataFrame({'rid':[sumtccashb.index[i][0] for i in range(0,len(sumtccashb))],'sic':[sumtccashb.index[i][1] for i in range(0,len(sumtccashb))],'amtcashb':[sumtccashb[j] for j in range(0,len(sumtccashb))]})
	df_ntc17 = pd.DataFrame({'rid':[numtc17.index[i][0] for i in range(0,len(numtc17))],'sic':[numtc17.index[i][1] for i in range(0,len(numtc17))],'ntc17':[numtc17[j] for j in range(0,len(numtc17))]})

	

	df1 = pd.merge(df_ntc10,df_amttc10,how='outer',on=['rid','sic'])
	df2 = pd.merge(df1,df_ntc13,how='outer',on=['rid','sic'])
	df3 = pd.merge(df2,df_amttc13,how='outer',on=['rid','sic'])
	df4 = pd.merge(df3,df_ncashb,how='outer',on=['rid','sic'])
	df5 = pd.merge(df4,df_amtcashb,how='outer',on=['rid','sic'])
	dfTrans = pd.merge(df5,df_ntc17,how='outer',on=['rid','sic'])

	dfTrans['ntc10']=dfTrans['ntc10'].fillna(0)
	dfTrans['amttc10']=dfTrans['amttc10'].fillna(0)
	dfTrans['ntc13']=dfTrans['ntc13'].fillna(0)
	dfTrans['amttc13']=dfTrans['amttc13'].fillna(0)
	dfTrans['ncashb']=dfTrans['ncashb'].fillna(0)
	dfTrans['amtcashb']=dfTrans['amtcashb'].fillna(0)
	dfTrans['ntc17']=dfTrans['ntc17'].fillna(0)

	arrTrans = [transMrg, dfTrans]

	return arrTrans


def computeReturn(transMrg, retrnDat):

	
	retrnDat['famt']=retrnDat['famt1']
	
	'''
	DataFrame for computing total number and amount of merchandise returned
	numreturn (n total return transaction)
	sumreturn (monetary amount of return transaction)
	retailers and customers are present in both data revenue and data return
	retailer id data revenue = retailer id data return
	customer id data revenue = customer id data return
	'''
	nrevGrp= transMrg.groupby(['strretailerid','sic','strcardno']).size()
	nretGrp= retrnDat.groupby(['strretailerid','sic','strcardno']).size()
	sretGrp= retrnDat.groupby(['strretailerid','sic','strcardno'])['famt'].sum()
	nrevInd= nrevGrp.index
	nretInd = nretGrp.index
	sretInd = sretGrp.index
	dfRev = pd.DataFrame({'rid':[nrevInd[i][0] for i in range(0,len(nrevGrp))],'sic':[nrevInd[i][1] for i in range(0,len(nrevGrp))],'cid':[nrevInd[i][2] for i in range(0,len(nrevGrp))]})
	
	dfRet1 = pd.DataFrame({'rid':[nretInd[i][0] for i in range(0,len(nretGrp))],'sic':[nretInd[i][1] for i in range(0,len(nretGrp))],'cid':[nretInd[i][2] for i in range(0,len(nretGrp))], 'nreturn':[nretGrp[j] for j in range(0,len(nretGrp))]})
	dfRet2 = pd.DataFrame({'rid':[sretInd[i][0] for i in range(0,len(sretGrp))],'sic':[sretInd[i][1] for i in range(0,len(sretGrp))],'cid':[sretInd[i][2] for i in range(0,len(sretGrp))], 'amtreturn': [sretGrp[j] for j in range(0,len(sretGrp))] })
	dfRet12 = pd.merge(dfRet1,dfRet2,how='outer',on=['rid','sic','cid'])
	dfMergeRet = pd.merge(dfRev,dfRet12,how='inner',on=['rid','sic','cid'])
	df_return = dfMergeRet[['rid','sic','nreturn','amtreturn']]

	df_return['nreturn']=df_return['nreturn'].fillna(0)
	df_return['amtreturn']=df_return['amtreturn'].fillna(0)

	return df_return

def computeFiid(transMrg):
	'''
	number and monetary value of transaction using different combination of acqfiid (i.e. ANZ or NONANZ), cardfiid (i.e. ANZ or NONANZ), and card type (i.e. credit or debit card)
	'''
	nFiidGrp = transMrg.groupby(['strretailerid','sic','acqfiid','cardfiid','t']).size()
	nFiidInd = nFiidGrp.index
	sFiidGrp = transMrg.groupby(['strretailerid','sic','acqfiid','cardfiid','t'])['famt'].sum()
	sFiidInd = sFiidGrp.index
	dfFiidNum = pd.DataFrame({'rid':[nFiidInd[i][0] for i in range(0,len(nFiidGrp))],'sic':[nFiidInd[i][1] for i in range(0,len(nFiidGrp))],'acqfiid':[nFiidInd[i][2] for i in range(0,len(nFiidGrp))],'cardfiid':[nFiidInd[i][3] for i in range(0,len(nFiidGrp))], 'cardtype':[nFiidInd[i][4] for i in range(0,len(nFiidGrp))], 'nfiid':[nFiidGrp[nFiidInd[j]] for j in range(0,len(nFiidGrp))]})

	dfFiidAmt = pd.DataFrame({'rid':[sFiidInd[i][0] for i in range(0,len(sFiidGrp))],'sic':[sFiidInd[i][1] for i in range(0,len(sFiidGrp))],'acqfiid':[sFiidInd[i][2] for i in range(0,len(sFiidGrp))],'cardfiid':[sFiidInd[i][3] for i in range(0,len(sFiidGrp))], 'cardtype':[sFiidInd[i][4] for i in range(0,len(sFiidGrp))], 'amtfiid': [sFiidGrp[sFiidInd[j]] for j in range(0,len(sFiidGrp))] })

	dfFiid = pd.merge(dfFiidNum, dfFiidAmt, how ='inner', on=['rid','sic','acqfiid','cardfiid','cardtype'])

	dfFiid1 = dfFiid[(dfFiid['acqfiid'] == 'ANZ') & (dfFiid['cardfiid'] == 'ANZ') & (dfFiid['cardtype'] == '1')]
	dfFiid1['nfiid1'] = dfFiid1['nfiid']
	dfFiid1['amtfiid1'] = dfFiid1['amtfiid']
	fiid1 = dfFiid1[['rid','sic','nfiid1','amtfiid1']]
	dfFiid2 = dfFiid[(dfFiid['acqfiid'] == 'ANZ') & (dfFiid['cardfiid'] == 'ANZ') & (dfFiid['cardtype'] == '2')]
	dfFiid2['nfiid2'] = dfFiid2['nfiid']
	dfFiid2['amtfiid2'] = dfFiid2['amtfiid']
	fiid2 = dfFiid2[['rid','sic','nfiid2','amtfiid2']]
	dfFiid3 = dfFiid[(dfFiid['acqfiid'] == 'ANZ') & (dfFiid['cardfiid'] == 'NONANZ') & (dfFiid['cardtype'] == '1')]
	dfFiid3['nfiid3'] = dfFiid3['nfiid']
	dfFiid3['amtfiid3'] = dfFiid3['amtfiid']
	fiid3 = dfFiid3[['rid','sic','nfiid3','amtfiid3']]
	dfFiid4 = dfFiid[(dfFiid['acqfiid'] == 'ANZ') & (dfFiid['cardfiid'] == 'NONANZ') & (dfFiid['cardtype'] == '2')]
	dfFiid4['nfiid4'] = dfFiid4['nfiid']
	dfFiid4['amtfiid4'] = dfFiid4['amtfiid']
	fiid4 = dfFiid4[['rid','sic','nfiid4','amtfiid4']]
	dfFiid5 = dfFiid[(dfFiid['acqfiid'] == 'NONANZ') & (dfFiid['cardfiid'] == 'ANZ') & (dfFiid['cardtype'] == '1')]
	dfFiid5['nfiid5'] = dfFiid5['nfiid']
	dfFiid5['amtfiid5'] = dfFiid5['amtfiid']
	fiid5 = dfFiid5[['rid','sic','nfiid5','amtfiid5']]
	dfFiid6 = dfFiid[(dfFiid['acqfiid'] == 'NONANZ') & (dfFiid['cardfiid'] == 'ANZ') & (dfFiid['cardtype'] == '2')]
	dfFiid6['nfiid6'] = dfFiid6['nfiid']
	dfFiid6['amtfiid6'] = dfFiid6['amtfiid']
	fiid6 = dfFiid6[['rid','sic','nfiid6','amtfiid6']]

	dfa = pd.merge(fiid1,fiid2,how='outer',on=['rid','sic'])
	dfb = pd.merge(dfa,fiid3,how='outer',on=['rid','sic'])
	dfc = pd.merge(dfb,fiid4,how='outer',on=['rid','sic'])
	dfd = pd.merge(dfc,fiid5,how='outer',on=['rid','sic'])
	dfFiidAll = pd.merge(dfd,fiid6,how='outer',on=['rid','sic'])

	dfFiidAll['nfiid1']=dfFiidAll['nfiid1'].fillna(0)
	dfFiidAll['nfiid2']=dfFiidAll['nfiid2'].fillna(0)
	dfFiidAll['nfiid3']=dfFiidAll['nfiid3'].fillna(0)
	dfFiidAll['nfiid4']=dfFiidAll['nfiid4'].fillna(0)
	dfFiidAll['nfiid5']=dfFiidAll['nfiid5'].fillna(0)
	dfFiidAll['nfiid6']=dfFiidAll['nfiid6'].fillna(0)
	dfFiidAll['amtfiid1']=dfFiidAll['amtfiid1'].fillna(0)
	dfFiidAll['amtfiid2']=dfFiidAll['amtfiid2'].fillna(0)
	dfFiidAll['amtfiid3']=dfFiidAll['amtfiid3'].fillna(0)
	dfFiidAll['amtfiid4']=dfFiidAll['amtfiid4'].fillna(0)
	dfFiidAll['amtfiid5']=dfFiidAll['amtfiid5'].fillna(0)
	dfFiidAll['amtfiid6']=dfFiidAll['amtfiid6'].fillna(0)


	return dfFiidAll


def computeRevenue(transMrg):

	'''
	max revenue
	time max revenue : morning, afternoon, night
	'''
	amtRev = transMrg.groupby(['strretailerid','sic'])['famt'].sum()
	ntrans = transMrg.groupby(['strretailerid','sic']).size()

	dfamtRev = pd.DataFrame({'rid':[amtRev.index[i][0] for i in range(0,len(amtRev))],'sic':[amtRev.index[i][1] for i in range(0,len(amtRev))], 'amtRev':[amtRev[j] for j in range(0,len(amtRev))]})
	dfntrans = pd.DataFrame({'rid':[ntrans.index[i][0] for i in range(0,len(ntrans))],'sic':[ntrans.index[i][1] for i in range(0,len(ntrans))], 'ntrans':[ntrans[j] for j in range(0,len(ntrans))]})
	maxRevDT = transMrg.groupby(['strretailerid','sic', 'dt', 'daytime'])['famt'].max()
	dfMaxRevDT = pd.DataFrame({'rid':[maxRevDT.index[i][0] for i in range(0,len(maxRevDT))],'sic':[maxRevDT.index[i][1] for i in range(0,len(maxRevDT))], 'timeOfDay':[maxRevDT.index[i][2] for i in range(0,len(maxRevDT))],'maxRev':[maxRevDT[j] for j in range(0,len(maxRevDT))]})
	maxRev = dfMaxRevDT.groupby(['rid','sic'])['maxRev'].max()
	dfMaxRev = pd.DataFrame({'rid':[maxRev.index[i][0] for i in range(0,len(maxRev))],'sic':[maxRev.index[i][1] for i in range(0,len(maxRev))],'maxRev':[maxRev[j] for j in range(0,len(maxRev))]})
	dfMergeMaxRev = pd.merge(dfMaxRevDT,dfMaxRev,how='inner',on=['rid','sic','maxRev'])


	mrg = pd.merge(dfamtRev,dfntrans,how='outer',on=['rid','sic'])
	dfRev = pd.merge(mrg,dfMergeMaxRev,how='outer',on=['rid','sic'])
	
	dfRev['amtRev']=dfRev['amtRev'].fillna(0)
	dfRev['ntrans']=dfRev['ntrans'].fillna(0)
	dfRev['timeOfDay']=dfRev['timeOfDay'].fillna(0)
	dfRev['maxRev']=dfRev['maxRev'].fillna(0)

	return dfRev


def computeDailySlope(datGrp):

	slopeDat = pd.DataFrame({'rid':[],'sic':[], 'intercept':[], 'slope':[]})
	for a in range(0,len(datGrp)):
		tmpArr = datGrp[a]
		arrLn = len(tmpArr)
		tmpArr = tmpArr.set_index(['strretailerid','sic'])
		dfTmp = pd.DataFrame({'rid':[tmpArr.index[b][0] for b in range(0,arrLn)],'sic':[tmpArr.index[c][1] for c in range(0,arrLn)], 'dt':[tmpArr['dt'][d] for d in range(0,arrLn)], 'famt':[tmpArr['famt'][e] for e in range(0,arrLn)]})
		dfTmpSorted = dfTmp.sort_index(by='dt', ascending=True)
		dfTmpSorted['sorted']=[f for f in range(0,len(dfTmpSorted))]
		x = dfTmpSorted['sorted']
		y = dfTmpSorted['famt']
		tmp1=[]
		tmp2=[]
		for g in range(0,len(x)):
			tmp1.append(x[g])
		for h in range(0,len(y)):
			tmp2.append(y[h])

		datX = np.array(tmp1)
		datY = np.array(tmp2)

		lm = LinearRegression()
		lm.fit(datX[:,np.newaxis],datY)
		intercept = lm.intercept_
		slope = lm.coef_[0]
		dfTheta = pd.DataFrame({'rid':(dfTmpSorted['rid'][0]),'sic':(dfTmpSorted['sic'][0]), 'intercept':[intercept], 'slope':[slope]})
		slopeDat = slopeDat.append(dfTheta, ignore_index=True)

	return slopeDat



def aggAll(dfAll):

	
	'''
	- dfTrans
	- dfMergeRet
	- dfFiidAll
	- dfRev
	- dfDailySlope
	'''
	
	dfTrans = dfAll[0]
	dfFiidAll = dfAll[1]
	dfMergeRet = dfAll[2]
	dfRev = dfAll[3]
	dfDailySlope = dfAll[4]

	agg1 = pd.merge(dfTrans,dfFiidAll,how='outer',on=['rid','sic'])
	agg2 = pd.merge(agg1,dfMergeRet,how='outer',on=['rid','sic'])
	agg3 = pd.merge(agg2,dfRev,how='outer',on=['rid','sic'])
	dataprep = pd.merge(agg3,dfDailySlope,how='outer',on=['rid','sic'])

	
	return dataprep


