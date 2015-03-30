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
arrFortnight= [df1,df2,df3]

for i in range(len(arrFortnight)):
	dat2w = pd.concat(arrFortnight[i], ignore_index=True)
	if i == 0:
		#compute initial contribution -- only for the first fortnight dataset
		dfCont = getInitCont(dat2w)

	#aggregated fortnightly data 	
	df2w = sumData(dat2w)
	
	#get ratio between first week and second week contribution 
	
	

	#change point

	#slope 2 weeks






