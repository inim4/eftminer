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

fileName = '/home/tita/docs/exp2/data%(i)02d.txt'
names = [fileName % {'i':i} for i in range(1,8)]
strDate = '2013-09-%(j)02d' 
dates = [strDate %{'j':j} for j in range(5,12)]

# Array list for week 1 dataset, week number is set manually (e.g. datList1, datList2,...)
# consists of 7 daily data, 1 week = 7 days
datList01 = []
for k in range(len(names)):

	dat = readData(names[k])

	# --------------------------
	# function selectData(arg1,arg2,nWeek)
	# nWeek is manually added
	slc = selectData(dat,dates[k],1)
	trans = selectTrans(slc)
	returnDat = trans[1]
	arrTrans = frameTrans(trans)

	# --------------------------
	# resulting 16 attributes from original transaction dataset
	# 'strretailerid','sic','strdate','dt','daytime','nWeek','acqfiid','cardfiid', 'strcardno','tc','t','aa','c','famt1','famt2', 'famt'
	datTrans = arrTrans[0]

	# --------------------------
	# resulting 9 attributes from total transaction and monetary 
	# 'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17'
	dfTrans = arrTrans[1]

	# --------------------------
	# resulting 4 attributes
	# 'rid', 'sic', 'nreturn', 'amtreturn'
	dfReturn = computeReturn(datTrans,returnDat)

	# --------------------------
	# resulting 14 attributes
	# 'rid','sic','nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6'
	dfFiid = computeFiid(datTrans)

	# --------------------------
	# computeRevenue(datTrans,nWeek), nWeek is manually added
	# resulting 8 attributes
	# 'rid','sic','dt','daytime','nWeek','ntrans','amtRev','maxRev'
	dfRev = computeRevenue(datTrans,1)
	
	# daily data
	# resulting 29 attributes from 4 data frames
	# 'rid', 'sic', 'ntc10', 'amttc10', 'ntc13', 'amttc13', 'ncashb', 'amtcashb', 'ntc17', 'nfiid1','amtfiid1','nfiid2','amtfiid2','nfiid3','amtfiid3','nfiid4','amtfiid4','nfiid5','amtfiid5','nfiid6','amtfiid6', 'nreturn', 'amtreturn', 'dt','daytime','nWeek','ntrans','amtRev','maxRev'
	arrDaily = [dfTrans,dfFiid,dfReturn,dfRev] 
	datDaily = aggAll(arrDaily)
			

	datList01.append(datDaily)


