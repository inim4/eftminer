from IPython.parallel import Client
import pandas as pd
import numpy as np
import math
import time
import datetime
from datetime import datetime
from timeit import default_timer as clock
from sklearn.linear_model import LinearRegression
from prep2 import *

fileName = '/home/tita/docs/exp2/data%(i)02d.txt'
names = [fileName % {'i':i} for i in range(1,8)]
strDate = '2013-09-%(j)02d' 
dates = [strDate %{'j':j} for j in range(5,12)]

datList = []
for k in range(len(names)):
	dat = readData(names[k])
	slc = selectData(dat,dates[k],k+1)
	trans = selectTrans(slc)
	arrTrans = frameTrans(trans)
	datTrans = arrTrans[0]
	dfTrans = arrTrans[1]

	dfReturn = computeReturn(datTrans)
	dfFiid = computeFiid(datTrans)
	dfRev = computeRevenue(datTrans)
	
	

	pieces = [dfTrans,dfFiid,dfReturn,dfRev,dfSlope] 
	dataprep = aggAll(pieces)
			

	datList.append(dataprep)


