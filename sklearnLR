import numpy as np
import pandas as pd
import scipy.stats as stats
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression

data1 = pd.read_table('tmpDatSorted.csv',sep=',',index_col=None,header=0,error_bad_lines=False,dtype = unicode);
x=data1['sorted']
y=data1['famt']
tmp1=[]
tmp2=[]
for i in range(0,len(x)):
	tmp1.append(x[i])
for j in range(0,len(y)):
	tmp2.append(y[j])

datX = np.array(tmp1)
datY = np.array(tmp2)
lm = LinearRegression()
lm.fit(datX[:,np.newaxis],datY)
LinearRegression(copy_X=True, fit_intercept=True, normalize=False)

#intercept
print lm.intercept_
66.7360227273

#slope
print lm.coef_[0]
1.53767595
