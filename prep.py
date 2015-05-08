import pandas as pd
import numpy as np
import math
import time
import datetime
from datetime import datetime
from scipy.stats.mstats import mode
from sklearn.linear_model import LinearRegression

def readData(fileDat):
	
	headstr=['strretailerid','str_time', 'str_sic','stracqfiid', 'strcardfiid','strcardno','strtrancde','famt1','famt2', 'tc','t','aa','c']
	dat = pd.read_table(fileDat,sep='\t',index_col=None,header=None,names=headstr,error_bad_lines=False,dtype = unicode); 
	alldata = dat[1:len(dat)]

	return alldata

def selectData(datparam, strdate, nweek):

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
	datparam['nWeek'] = nweek
	#datparam['famt1'] = datparam['famt1'].astype(float)
	#datparam['famt2'] = datparam['famt2'].astype(float)
	datparam['sic'] = [sic[:2] for sic in datparam['str_sic']]
	siclist=['48','50','52','53','54','55','56','57','58','59','63','70','72','73','75','78']
	cardlist=['1','2']
	dat = datparam[(datparam['sic'].isin(siclist)) & (datparam['t'].isin(cardlist))]
	del datparam
	dat['acqfiid']= np.where(dat['stracqfiid'].str.contains('ANZ'),'ANZ','NONANZ')
	dat['cardfiid']= np.where(dat['strcardfiid'].str.contains('ANZ'),'ANZ','NONANZ')
	slcDat = dat[['strretailerid','sic','str_time','strdate','nWeek','acqfiid','cardfiid', 'strcardno','tc','t','aa','c','famt1','famt2']]
	del dat
	
	
	
	return slcDat

def selectTrans(dat):

	'''
	convert / discretize datetime to time of a day
	morning (1) -- 00.01 - 12.00
	noon (2) -- 12.00 - 18.00
	evening (3) -- 18.00 - 24.00
	'''
	dat['str_time']=dat['str_time'].apply(lambda x:str(x).replace('.',''))
	dattmp = dat[dat['str_time'].str.len()==12]
	del dat
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
	del dattmp
	datFilter['mergeseconds']=datFilter['ssInt'] + datFilter['fms']
	datFilter['secstr'] = datFilter['mergeseconds'].apply(lambda x: '0'+'%.6f' % float(x) if x<10 else '%.6f' % float(x))
	datFilter['strdatetime'] = datFilter['strdate'] + ' ' + datFilter['hh'] + ':' + datFilter['mm'] + ':' + datFilter['secstr']
	datFilter['dt']=datFilter['strdatetime'].apply(lambda x:(datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')))
	
	datTrans = datFilter[['strretailerid','sic','strdate','dt','nWeek','acqfiid','cardfiid', 'strcardno','tc','t','aa','c','famt1','famt2']]
	
	return datTrans
	
def getDayTime(datTrans):

	datTrans['is_morning']=datTrans['dt'].apply(lambda x: 1 if x.hour < 12 else 0)
	datTrans['is_noon']=datTrans['dt'].apply(lambda x: 2 if (x.hour > 12 and x.hour <=18)  else 0)
	datTrans['is_evening']=datTrans['dt'].apply(lambda x: 3 if (x.hour > 18 and x.hour <=24) else 0)
	datTrans['daytime'] = datTrans['is_morning'].apply(lambda x: int(x)) + datTrans['is_noon'].apply(lambda x: int(x)) + datTrans['is_evening'].apply(lambda x: int(x))

	dfDayTime = datTrans[['strretailerid','sic','strdate','dt','daytime','nWeek','acqfiid','cardfiid', 'strcardno','tc','t','aa','c','famt1','famt2']]
	
	return dfDayTime

def filterSic(dfDayTime):

	cdepos=['10','12','13', '17', '18','21','24','62']
	cdemin=['14','22']
	
	rev = dfDayTime[dfDayTime['tc'].isin(cdepos)]
	retrn = dfDayTime[dfDayTime['tc'].isin(cdemin)]
	del dfDayTime
	datTrans =[rev,retrn]
	del rev,retrn

	return datTrans
	
def frameTrans(rev):

	

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
	del tc10,tc13,tc_cashb,tc17,tc_auth,tc21
	del transArr

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
	del df_ntc10,df_amttc10,df_ntc13,df_amttc13,df_ncashb,df_amtcashb,df_ntc17,df1,df2,df3,df4,df5

	dfTrans['ntc10']=dfTrans['ntc10'].fillna(0)
	dfTrans['amttc10']=dfTrans['amttc10'].fillna(0)
	dfTrans['ntc13']=dfTrans['ntc13'].fillna(0)
	dfTrans['amttc13']=dfTrans['amttc13'].fillna(0)
	dfTrans['ncashb']=dfTrans['ncashb'].fillna(0)
	dfTrans['amtcashb']=dfTrans['amtcashb'].fillna(0)
	dfTrans['ntc17']=dfTrans['ntc17'].fillna(0)

	arrTrans = [transMrg, dfTrans]
	del transMrg,dfTrans

	return arrTrans

def computeReturn(transMrg, returnDat):
	
	returnDat['famt']=returnDat['famt1']
	'''
	DataFrame for computing total number and amount of merchandise returned
	numreturn (n total return transaction)
	sumreturn (monetary amount of return transaction)
	retailers and customers are present in both data revenue and data return
	retailer id data revenue = retailer id data return
	customer id data revenue = customer id data return
	'''
	nrevGrp= transMrg.groupby(['strretailerid','sic','strcardno']).size()
	nretGrp= returnDat.groupby(['strretailerid','sic','strcardno']).size()
	sretGrp= returnDat.groupby(['strretailerid','sic','strcardno'])['famt'].sum()
	del transMrg,returnDat
	
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
	
	del nrevGrp,nretGrp,sretGrp
	del dfRet1,dfRet2,dfRev,dfRet12,dfMergeRet

	return df_return

def computeFiid(transMrg):
	'''
	number and monetary value of transaction using different combination of acqfiid (i.e. ANZ or NONANZ), cardfiid (i.e. ANZ or NONANZ), and card type (i.e. credit or debit card)
	'''
	nFiidGrp = transMrg.groupby(['strretailerid','sic','acqfiid','cardfiid','t']).size()
	nFiidInd = nFiidGrp.index
	sFiidGrp = transMrg.groupby(['strretailerid','sic','acqfiid','cardfiid','t'])['famt'].sum()
	sFiidInd = sFiidGrp.index
	
	del transMrg
	
	dfFiidNum = pd.DataFrame({'rid':[nFiidInd[i][0] for i in range(0,len(nFiidGrp))],'sic':[nFiidInd[i][1] for i in range(0,len(nFiidGrp))],'acqfiid':[nFiidInd[i][2] for i in range(0,len(nFiidGrp))],'cardfiid':[nFiidInd[i][3] for i in range(0,len(nFiidGrp))], 'cardtype':[nFiidInd[i][4] for i in range(0,len(nFiidGrp))], 'nfiid':[nFiidGrp[nFiidInd[j]] for j in range(0,len(nFiidGrp))]})

	dfFiidAmt = pd.DataFrame({'rid':[sFiidInd[i][0] for i in range(0,len(sFiidGrp))],'sic':[sFiidInd[i][1] for i in range(0,len(sFiidGrp))],'acqfiid':[sFiidInd[i][2] for i in range(0,len(sFiidGrp))],'cardfiid':[sFiidInd[i][3] for i in range(0,len(sFiidGrp))], 'cardtype':[sFiidInd[i][4] for i in range(0,len(sFiidGrp))], 'amtfiid': [sFiidGrp[sFiidInd[j]] for j in range(0,len(sFiidGrp))] })

	dfFiid = pd.merge(dfFiidNum, dfFiidAmt, how ='inner', on=['rid','sic','acqfiid','cardfiid','cardtype'])
	del dfFiidNum,dfFiidAmt
	del nFiidGrp,nFiidInd,sFiidGrp,sFiidInd

	dfFiid1 = dfFiid[(dfFiid['acqfiid'] == 'ANZ') & (dfFiid['cardfiid'] == 'ANZ') & (dfFiid['cardtype'] == '1')]
	dfFiid1['nfiid1'] = dfFiid1['nfiid']
	dfFiid1['amtfiid1'] = dfFiid1['amtfiid']
	fiid1 = dfFiid1[['rid','sic','nfiid1','amtfiid1']]
	del dfFiid1
	dfFiid2 = dfFiid[(dfFiid['acqfiid'] == 'ANZ') & (dfFiid['cardfiid'] == 'ANZ') & (dfFiid['cardtype'] == '2')]
	dfFiid2['nfiid2'] = dfFiid2['nfiid']
	dfFiid2['amtfiid2'] = dfFiid2['amtfiid']
	fiid2 = dfFiid2[['rid','sic','nfiid2','amtfiid2']]
	del dfFiid2
	dfFiid3 = dfFiid[(dfFiid['acqfiid'] == 'ANZ') & (dfFiid['cardfiid'] == 'NONANZ') & (dfFiid['cardtype'] == '1')]
	dfFiid3['nfiid3'] = dfFiid3['nfiid']
	dfFiid3['amtfiid3'] = dfFiid3['amtfiid']
	fiid3 = dfFiid3[['rid','sic','nfiid3','amtfiid3']]
	del dfFiid3
	dfFiid4 = dfFiid[(dfFiid['acqfiid'] == 'ANZ') & (dfFiid['cardfiid'] == 'NONANZ') & (dfFiid['cardtype'] == '2')]
	dfFiid4['nfiid4'] = dfFiid4['nfiid']
	dfFiid4['amtfiid4'] = dfFiid4['amtfiid']
	fiid4 = dfFiid4[['rid','sic','nfiid4','amtfiid4']]
	del dfFiid4
	dfFiid5 = dfFiid[(dfFiid['acqfiid'] == 'NONANZ') & (dfFiid['cardfiid'] == 'ANZ') & (dfFiid['cardtype'] == '1')]
	dfFiid5['nfiid5'] = dfFiid5['nfiid']
	dfFiid5['amtfiid5'] = dfFiid5['amtfiid']
	fiid5 = dfFiid5[['rid','sic','nfiid5','amtfiid5']]
	del dfFiid5
	dfFiid6 = dfFiid[(dfFiid['acqfiid'] == 'NONANZ') & (dfFiid['cardfiid'] == 'ANZ') & (dfFiid['cardtype'] == '2')]
	dfFiid6['nfiid6'] = dfFiid6['nfiid']
	dfFiid6['amtfiid6'] = dfFiid6['amtfiid']
	fiid6 = dfFiid6[['rid','sic','nfiid6','amtfiid6']]
	del dfFiid6
	dfa = pd.merge(fiid1,fiid2,how='outer',on=['rid','sic'])
	dfb = pd.merge(dfa,fiid3,how='outer',on=['rid','sic'])
	dfc = pd.merge(dfb,fiid4,how='outer',on=['rid','sic'])
	dfd = pd.merge(dfc,fiid5,how='outer',on=['rid','sic'])
	dfFiidAll = pd.merge(dfd,fiid6,how='outer',on=['rid','sic'])
	del dfFiid,fiid1,fiid2,fiid3,fiid4,fiid5,fiid6,dfa,dfb,dfc,dfd

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

def computeRevenue(transMrg, nWeek):

	'''
	max revenue
	time max revenue : morning, afternoon, night
	'''
	amtRev = transMrg.groupby(['strretailerid','sic'])['famt'].sum()
	ntrans = transMrg.groupby(['strretailerid','sic']).size()
	
	

	dfamtRev = pd.DataFrame({'rid':[amtRev.index[i][0] for i in range(0,len(amtRev))],'sic':[amtRev.index[i][1] for i in range(0,len(amtRev))], 'amtRev':[amtRev[j] for j in range(0,len(amtRev))]})
	dfntrans = pd.DataFrame({'rid':[ntrans.index[i][0] for i in range(0,len(ntrans))],'sic':[ntrans.index[i][1] for i in range(0,len(ntrans))], 'ntrans':[ntrans[j] for j in range(0,len(ntrans))]})

	#calculate daily max revenue
	maxRevDT = transMrg.groupby(['strretailerid','sic', 'dt', 'daytime'])['famt'].max()
	dfMaxRevDT = pd.DataFrame({'rid':[maxRevDT.index[i][0] for i in range(0,len(maxRevDT))],'sic':[maxRevDT.index[i][1] for i in range(0,len(maxRevDT))], 'dt':[maxRevDT.index[i][2] for i in range(0,len(maxRevDT))], 'daytime':[maxRevDT.index[i][3] for i in range(0,len(maxRevDT))], 'maxRev':[maxRevDT[j] for j in range(0,len(maxRevDT))]})
	maxRev = dfMaxRevDT.groupby(['rid','sic'])['maxRev'].max()
	dfMaxRev = pd.DataFrame({'rid':[maxRev.index[i][0] for i in range(0,len(maxRev))],'sic':[maxRev.index[i][1] for i in range(0,len(maxRev))],'maxRev':[maxRev[j] for j in range(0,len(maxRev))]})
	dfMergeMaxRev = pd.merge(dfMaxRevDT,dfMaxRev,how='inner',on=['rid','sic','maxRev'])

	del amtRev,ntrans,transMrg
	del maxRev,maxRevDT
	mrg = pd.merge(dfamtRev,dfntrans,how='outer',on=['rid','sic'])
	dfRev = pd.merge(mrg,dfMergeMaxRev,how='outer',on=['rid','sic'])
	del dfamtRev,dfntrans,mrg,dfMergeMaxRev,dfMaxRevDT,dfMaxRev
	
	dfRev['amtRev']=dfRev['amtRev'].fillna(0)
	dfRev['ntrans']=dfRev['ntrans'].fillna(0)
	dfRev['maxRev']=dfRev['maxRev'].fillna(0)
	dfRev['nWeek']=nWeek

	return dfRev

def aggAll(dfAll):

	
	dfTrans = dfAll[0]
	dfFiid = dfAll[1]
	dfRet = dfAll[2]
	dfRev = dfAll[3]
	dfAll=None

	agg1 = pd.merge(dfTrans,dfFiid,how='outer',on=['rid','sic'])
	agg2 = pd.merge(agg1,dfRet,how='outer',on=['rid','sic'])
	dataprep = pd.merge(agg2,dfRev,how='outer',on=['rid','sic'])
	del dfTrans,dfFiid,dfRet,dfRev,agg1,agg2

	dataprep['ntc10']=dataprep['ntc10'].fillna(0)
	dataprep['amttc10']=dataprep['amttc10'].fillna(0)
	dataprep['ntc13']=dataprep['ntc13'].fillna(0)
	dataprep['amttc13']=dataprep['amttc13'].fillna(0)
	dataprep['ncashb']=dataprep['ncashb'].fillna(0)
	dataprep['amtcashb']=dataprep['amtcashb'].fillna(0)
	dataprep['ntc17']=dataprep['ntc17'].fillna(0)
	dataprep['nreturn']=dataprep['nreturn'].fillna(0)
	dataprep['amtreturn']=dataprep['amtreturn'].fillna(0)
	dataprep['nfiid1']=dataprep['nfiid1'].fillna(0)
	dataprep['nfiid2']=dataprep['nfiid2'].fillna(0)
	dataprep['nfiid3']=dataprep['nfiid3'].fillna(0)
	dataprep['nfiid4']=dataprep['nfiid4'].fillna(0)
	dataprep['nfiid5']=dataprep['nfiid5'].fillna(0)
	dataprep['nfiid6']=dataprep['nfiid6'].fillna(0)
	dataprep['amtfiid1']=dataprep['amtfiid1'].fillna(0)
	dataprep['amtfiid2']=dataprep['amtfiid2'].fillna(0)
	dataprep['amtfiid3']=dataprep['amtfiid3'].fillna(0)
	dataprep['amtfiid4']=dataprep['amtfiid4'].fillna(0)
	dataprep['amtfiid5']=dataprep['amtfiid5'].fillna(0)
	dataprep['amtfiid6']=dataprep['amtfiid6'].fillna(0)
	dataprep['amtRev']=dataprep['amtRev'].fillna(0)
	dataprep['ntrans']=dataprep['ntrans'].fillna(0)
	dataprep['maxRev']=dataprep['maxRev'].fillna(0)
	
	return dataprep

# --------------------------------------------------------------
# function to aggregate attributes
# --------------------------------------------------------------
def sumData(dfDat):

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
	
	del ntc10,amttc10,ntc13,amttc13,ncashb,amtcashb,nreturn,amtreturn,ntc17

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
	
	del nfiid1,amtfiid1,nfiid2,amtfiid2,nfiid3,amtfiid3,nfiid4,amtfiid4,nfiid5,amtfiid5,nfiid6,amtfiid6

	df1 = pd.merge(df_ntc10,df_amttc10,how='outer',on=['rid','sic'])
	df2 = pd.merge(df_ntc13,df_amttc13,how='outer',on=['rid','sic'])
	df3 = pd.merge(df_ncashb,df_amtcashb,how='outer',on=['rid','sic'])
	df4 = pd.merge(df_nreturn,df_amtreturn,how='outer',on=['rid','sic'])
	df5 = pd.merge(df1,df2,how='outer',on=['rid','sic'])
	df6 = pd.merge(df5,df3,how='outer',on=['rid','sic'])
	df7 = pd.merge(df6,df4,how='outer',on=['rid','sic'])
	df8 = pd.merge(df7,df_ntc17,how='outer',on=['rid','sic'])
	
	del df_ntc10,df_amttc10,df_ntc13,df_amttc13,df_ncashb,df_amtcashb,df_nreturn,df_amtreturn,df_ntc17
	del df1,df2,df3,df4,df5,df6,df7

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
	
	del df_nfiid1,df_amtfiid1,df_nfiid2,df_amtfiid2,df_nfiid3,df_amtfiid3,df_nfiid4,df_amtfiid4,df_nfiid5,df_amtfiid5,df_nfiid6,df_amtfiid6
	del df9,df10,df11,df12,df13,df14,df15,df16,df17,df18
	
	df20 = pd.merge(df8,df19,how='outer',on=['rid','sic'])
	df21 = pd.merge(df20,df_amtrev,how='outer',on=['rid','sic'])
	groupedDat = pd.merge(df21,df_ntrans,how='outer',on=['rid','sic'])
	
	del df8,df19,df20,df21,df_amtrev,df_ntrans
	
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
	groupedDat['nfiid4']=groupedDat['nfiid4'].fillna(0)
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


# ----------------------------------------------------------------------------------
# function to compute maximum revenue for weekly data
# ----------------------------------------------------------------------------------
def weeklyMaxRevenue(dfDat,nWeek):
	
	maxRev = dfDat.groupby(['rid','sic'])['maxRev'].max()
	dfMaxRev = pd.DataFrame({'rid':[maxRev.index[i][0] for i in range(0,len(maxRev))],'sic':[maxRev.index[i][1] for i in range(0,len(maxRev))],'maxRev':[maxRev[j] for j in range(0,len(maxRev))]})
	dfMaxRev['nWeek']=nWeek

	return dfMaxRev



# ----------------------------------------------------------------------------------
# function to compute maximum revenue for fortnightly data
# ----------------------------------------------------------------------------------
def fortnightMaxRevenue(dfDat):
	
	maxRev = dfDat.groupby(['rid','sic','nWeek'])['maxRev'].max()
	df2wMaxRev = pd.DataFrame({'rid':[maxRev.index[i][0] for i in range(0,len(maxRev))],'sic':[maxRev.index[i][1] for i in range(0,len(maxRev))],'nWeek':[maxRev.index[i][2] for i in range(0,len(maxRev))],'maxRev':[maxRev[j] for j in range(0,len(maxRev))]})
	
	return df2wMaxRev

# ----------------------------------------------------------------------------------
# function to compute maximum revenue for all 6 weeks data
# ----------------------------------------------------------------------------------
def getMaxRevenue(dfDat):
	
	maxRev = dfDat.groupby(['rid','sic'])['maxRev'].max()
	dfMaxRev = pd.DataFrame({'rid':[maxRev.index[i][0] for i in range(0,len(maxRev))],'sic':[maxRev.index[i][1] for i in range(0,len(maxRev))],'maxRev':[maxRev[j] for j in range(0,len(maxRev))]})


	return dfMaxRev

# -------------------------------------------------------------------------
# function to compute the slope attributes (slope info, intercept, gradient)
# -------------------------------------------------------------------------
def getSlope(datGrp):

	slopeDat = pd.DataFrame({'rid':[],'sic':[], 'intercept':[], 'slope':[]})
	for iNum in range(0,len(datGrp)):
		tmpArr = datGrp[iNum]
		arrLn = len(tmpArr)
		tmpArr = tmpArr.set_index(['rid','sic'])
		dfTmp = pd.DataFrame({'rid':[tmpArr.index[jNum][0] for jNum in range(0,arrLn)],'sic':[tmpArr.index[jNum][1] for jNum in range(0,arrLn)], 'dt':[tmpArr['dt'][jNum] for jNum in range(0,arrLn)], 'amtRev':[tmpArr['amtRev'][jNum] for jNum in range(0,arrLn)]})
		dfTmpSorted = dfTmp.sort_index(by='dt', ascending=True)
		dfTmpSorted['sorted']=[kNum for kNum in range(0,len(dfTmpSorted))]
		del tmpArr,dfTmp
		
		x = dfTmpSorted['sorted']
		y = dfTmpSorted['amtRev']
		tmp1=[]
		tmp2=[]
		for lNum in range(0,len(x)):
			tmp1.append(x[lNum])
		for mNum in range(0,len(y)):
			tmp2.append(y[mNum])

		datX = np.array(tmp1)
		datY = np.array(tmp2)
		del x,y

		lm = LinearRegression()
		lm.fit(datX[:,np.newaxis],datY)
		intercept = lm.intercept_
		slope = lm.coef_[0]
		dfTheta = pd.DataFrame({'rid':(dfTmpSorted['rid'][0]),'sic':(dfTmpSorted['sic'][0]), 'intercept':[intercept], 'slope':[slope]})
		slopeDat = slopeDat.append(dfTheta, ignore_index=True)
		
		del dfTmpSorted,dfTheta


	return slopeDat

# -------------------------------------------------------------------------
# function to compute the mode of daytime when max revenue occurs
# -------------------------------------------------------------------------
def getModeTime(dfDat):
	
	# find the most frequent time of day and datetime when max revenue occurs
	# from all merged 6W data       
	
	# mode of timeOfDay for every rid+sic key
	grpTimeOfDay = dfDat.groupby(['rid','sic'])
	grpName=[]
	tmpArr=[]
	modeTime = 0
	dfModeTime = pd.DataFrame({'rid':[],'sic':[], 'timeMaxRev':[]})
	for name, group in grpTimeOfDay:
		grpName.append(group)
	for j in range(len(grpName)):
		tmpArr = grpName[j]
		tmpArr = tmpArr.set_index(['rid', 'sic'])
		modeTmp = mode(tmpArr['daytime'])
		modeTime = modeTmp[0][0]
		dfTimeOfDay = pd.DataFrame({'rid':(tmpArr.index[0][0]),'sic':(tmpArr.index[0][1]), 'timeMaxRev':(modeTime)})
		dfModeTime = dfModeTime.append(dfTimeOfDay, ignore_index=True)
	del tmpArr,modeTmp,modeTime,dfTimeOfDay,grpTimeOfDay,grpName
	return dfModeTime

# -------------------------------------------------------------------------
# function to compute the mode of week when max revenue occurs
# -------------------------------------------------------------------------
def getModeWeek(dfDat):
	
	# find the most frequent week when max revenue occurs : 1 = week 1; 2 = week 2
	# from all merged 6w data
	dfDat['week'] = np.where((dfDat['nWeek']%2==0),2,1)
	datWeek = dfDat[['rid', 'sic', 'week']]
	# mode of timeOfDay for every rid+sic key
	grpWeekRev = datWeek.groupby(['rid','sic'])
	grpName=[]
	tmpArr=[]
	modeWeek = 0
	dfModeWeek = pd.DataFrame({'rid':[],'sic':[], 'weekMaxRev':[]})
	for name, group in grpWeekRev:
		grpName.append(group)
	for j in range(len(grpName)):
		tmpArr = grpName[j]
		tmpArr = tmpArr.set_index(['rid', 'sic'])
		modeTmp = mode(tmpArr['week'])
		modeWeek = modeTmp[0][0]
		dfWeek = pd.DataFrame({'rid':(tmpArr.index[0][0]),'sic':(tmpArr.index[0][1]), 'weekMaxRev':(modeWeek)})
		dfModeWeek = dfModeWeek.append(dfWeek, ignore_index=True)
	del tmpArr,modeTmp,modeWeek,dfWeek,grpWeekRev,grpName,dfDat,datWeek
	return dfModeWeek


# --------------------------------------------------------------
# function to compute initial contribution in the first two week
# --------------------------------------------------------------
def getInitCont(dfDat):

	# get initial contribution (only first fortnightly revenue of training data)
	# initial contribution is the sum of fortnight revenue in the first two week of observed data
	amtRev = dfDat.groupby(['rid','sic'])['amtRev'].sum()
	dfInitCont = pd.DataFrame({'rid':[amtRev.index[i][0] for i in range(0,len(amtRev))],'sic':[amtRev.index[i][1] for i in range(0,len(amtRev))], 'initCont':[amtRev[j] for j in range(0,len(amtRev))]})

	return dfInitCont

# -----------------------------------------------------------------
# function to compute the ratio of contribution between week 1 & 2
# -----------------------------------------------------------------
def getContRatio(datGrp):
	
	# get first week and second week contribution within first fortnight data
	dfContRatio = pd.DataFrame({'rid':[],'sic':[], 'contRatio':[]})
	tmpArr=[]
	fCont = 0
	lCont = 0
	contRatio = 0
	for iNum in range(0,len(datGrp)):
		tmpArr = datGrp[iNum]
		arrLn = len(tmpArr)
		tmpArr = tmpArr.set_index(['rid','sic'])
	
		# get value of fCont and lCont from first and second week of revenue
		# if retailer data (rid, sic) appears in both weeks (week 1 & 2)
		if arrLn==2:
			fCont = tmpArr['amtRev'][0]
			lCont = tmpArr['amtRev'][1]
			tmpCont = fCont/lCont
			#ratio contribution = 3 if revenue in first week > revenue in second week, otherwise 4
			if tmpCont > 1:
				contRatio = 3
			else:
				contRatio = 4

			

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
		dfTmp = pd.DataFrame({'rid':[(tmpArr.index[0][0])],'sic':[(tmpArr.index[0][1])], 'contRatio':[(contRatio)]})
		dfContRatio = dfContRatio.append(dfTmp, ignore_index=True)
		
	del tmpArr,dfTmp
	return dfContRatio

# ---------------------------------------------------------------------------------
# function to get the change point between first and second week slope
# ---------------------------------------------------------------------------------
def getChangePoint(dfSlope):
	
	dfChangePoint = pd.DataFrame({'rid':[],'sic':[],'changePoint':[], 'nWeek':[]})
	retailerGrp = dfSlope.groupby(['rid','sic'])
	grpName=[]
	dfFortnight=[]
	changePoint = 0 
	nWeek = 0
	
	for name, group in retailerGrp:
		grpName.append(group)

	for j in range(len(grpName)):
		grpFortnight = grpName[j]
		fortnightLn = len(dfFortnight)
		grpFortnight = dfFortnight.sort_index(by='nWeek', ascending=True)
		grpFortnight = dfFortnight.set_index(['rid','sic'])
		# if retailer data (rid, sic) appears in both weeks (week 1 & 2)
		if dfFortnightLn==2:
			
			# slope positive - negative
			if ((grpFortnight['weeklySlope'][0] == 1) & (grpFortnight['weeklySlope'][1] == 2)):
				changePoint = 1
			# slope negative - positive
			elif ((grpFortnight['weeklySlope'][0] == 2) & (grpFortnight['weeklySlope'][1] == 1)):
				changePoint = 2
			# slope positive - positive
			elif ((grpFortnight['weeklySlope'][0] == 1) & (grpFortnight['weeklySlope'][1] == 1)):
				changePoint = 3
			# slope negative - negative 
			elif ((grpFortnight['weeklySlope'][0] == 2) & (grpFortnight['weeklySlope'][1] == 2)):
				changePoint = 4

		# if retailer data only appears once within fortnight data
		else:
			
			# if it only appears in week 1
			# slope positive - flat
			if ((grpFortnight['nWeek'][0] == 1) & (grpFortnight['weeklySlope'][1] == 1)):
				changePoint = 5
			# slope negative - flat
			elif ((grpFortnight['nWeek'][0] == 1) & (grpFortnight['weeklySlope'][1] == 2)):
				changePoint = 6
			
			# if it only appears in week 2
			# slope flat - positive
			elif ((grpFortnight['nWeek'][0] == 2) & (grpFortnight['weeklySlope'][1] == 1)):
				changePoint = 7
			# slope flat - negative
			elif ((grpFortnight['nWeek'][0] == 2) & (grpFortnight['weeklySlope'][1] == 2)):
				changePoint = 8
			
	df = pd.DataFrame({'rid':(dfFortnight['rid'][0]),'sic':(dfFortnight['sic'][0]), 'changePoint': changePoint})
	dfChangePoint = dfChangePoint.append(df, ignore_index = True)
	
	del grpFortnight,retailerGrp,grpName,df
	return dfChangePoint

# ---------------------------------------------------------------------------------
# function to get the combination of 2 weeks change point and fortnightly slope
# ---------------------------------------------------------------------------------
def combineChangePoint(df2WChangePoint, df2WSlope):

	dfMrg = pd.merge(df2WChangePoint,df2WSlope, on=['rid','sic'])
	
	dfMrg['combineSlope']=np.where((dfMrg['slopeInfo'] == 1),(dfMrg['changePoint']),((dfMrg['changePoint'])+8)) 
	grp1=[2,3,5,7]
	grp2=[9,12,14,16]
	grp3=[1,4,6,8]
	grp4=[10,11,13,15]
	combine1 = dfMrg[dfMrg['combineSlope'].isin(grp1)]
	combine2 = dfMrg[dfMrg['combineSlope'].isin(grp2)]
	combine3 = dfMrg[dfMrg['combineSlope'].isin(grp3)]
	combine4 = dfMrg[dfMrg['combineSlope'].isin(grp4)]
	combine1['grpCombineSlope'] = 1 
	combine2['grpCombineSlope'] = 2
	combine3['grpCombineSlope'] = 3
	combine4['grpCombineSlope'] = 4
	arrCombine = [combine1,combine2,combine3,combine4]
	mrgCombine = pd.concat(arrCombine, ignore_index=True)
	dfCombineCP = mrgCombine[['rid', 'sic', 'grpCombineSlope']]
	
	del mrgCombine,arrCombine,combine1,combine2,combine3,combine4,dfMrg,grp1,grp2,grp3,grp4,df2WChangePoint,df2WSlope

	return dfCombineCP

# ------------------------------------------------------------------------------------------
# function to get the mode of the combination b/w 2 weeks change point and fortnightly slope
# ------------------------------------------------------------------------------------------
def getModeCombineCP(dfCombineCP):
	
	# mode of combination
	dfCombine = dfCombineCP.groupby(['rid','sic'])
	grpName=[]
	tmpArr=[]
	modeComb = 0
	dfModeCP = pd.DataFrame({'rid':[],'sic':[], 'combineSlope':[]})
	for name, group in dfCombine:
		grpName.append(group)
	for j in range(len(grpName)):
		tmpArr = grpName[j]
		tmpArr = tmpArr.set_index(['rid', 'sic'])
		modeTmp = mode(tmpArr['combineSlope'])
		modeCP = modeTmp[0][0]
		dfCP = pd.DataFrame({'rid':(tmpArr.index[0][0]),'sic':(tmpArr.index[0][1]), 'combineSlope':(modeCP)})
		dfModeCP = dfModeCP.append(dfCP, ignore_index=True)
	
	del tmpArr, modeTmp,modeCP,dfCP,dfCombine,grpName,dfCombineCP
	return dfModeCP


def mergeFortnightly(dfDat):

	#get fortnightly slope
	df2WSlope = getSlope()

	return dfFortnightly

def mergeAll(dfDat):

	#aggregate cumulative data

	#get 6w slope from all merged 6w data
	getSlope()
	dfTimeMaxRev = getModeTime()
	dfWeekMaxRev = getModeWeek()

	return dfAll
