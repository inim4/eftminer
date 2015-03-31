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

for i in range(len(arrFortnight)):
	dat2w = pd.concat(arrFortnight[i], ignore_index=True)
	datCont = dat2w[['rid','sic','nWeek','amtRev']]
	datSlope = dat2w[['rid','sic','nWeek','slopeInfo']]
	if i == 0:
		#compute initial contribution -- only for the first fortnight dataset
		dfCont = getInitCont(datCont)
	
	#get ratio between first week and second week contribution
	dfContRatio = getContRatio(datCont)

	#get change point of weekly slope within fortnightly dataset
	dfChangePoint = getChangePoint(datSlope)

	#aggregated fortnightly data 	
	df2w = sumData(dat2w)
	
	 
	
	

	#change point

	#slope 2 weeks






