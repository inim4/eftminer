from IPython.parallel import Client
import pandas as pd
import numpy as np
import math
import time
import datetime
from datetime import datetime
from timeit import default_timer as clock
from prep import *




# Connect to the IPython cluster
c = Client(profile='titaClusters')
c[:].run('prep.py')
v = c[:]
n =len(c)
v.block=True


fileName = '/home/tita/docs/readdata/data%(i)02d.txt'
names = [fileName % {'i':i} for i in range(1,8)]
strDate = '2013-09-%(j)02d' 
dates = [strDate %{'j':j} for j in range(5,12)]



t1 = clock()
dat1 = c[0].apply_async(readDat,names[0])
dat2 = c[1].apply_async(readDat,names[1])
dat3 = c[2].apply_async(readDat,names[2])
dat4 = c[3].apply_async(readDat,names[3])
dat5 = c[4].apply_async(readDat,names[4])
dat6 = c[5].apply_async(readDat,names[5])
dat7 = c[6].apply_async(readDat,names[6])

t2 = clock()
print("Speed processing: ", (t2-t1))

#t3 = clock()
#combinedDf = pd.concat(dat)
#t4 = clock()
#print("Speed combining: ", (t4-t3))
