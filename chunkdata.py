from IPython.parallel import Client
import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO
import pandas as pd
import numpy as np
import re
from chunk import *


    
def extract_data(dataNames):

    datRead = read_data(dataNames)
    datClean = clean_data(datRead)

    return datClean

fileName = '/mnt/eftdata2/pos_ptlf_201309%(i)02d.txt'
names = [fileName % {'i':i} for i in range(5,12)]
strDate = '2013-09-%(j)02d' 
dates = [strDate %{'j':j} for j in range(5,12)]


# Connect to the IPython cluster
# set 7 cluster engines, 1 cluster preprocess 1 daily data (total 7 daily dataset per week)
c = Client(profile='titaClusters')
c[:].run('chunk.py')
c[:].block = True
numC = len(c)

dat01 = c[0].apply_sync(extract_data,names[0])
dat02 = c[1].apply_sync(extract_data,names[1])
dat03 = c[2].apply_sync(extract_data,names[2])
dat04 = c[3].apply_sync(extract_data,names[3])
dat05 = c[4].apply_sync(extract_data,names[4])
dat06 = c[5].apply_sync(extract_data,names[5])
dat07 = c[6].apply_sync(extract_data,names[6])


# Array list for week 1 dataset, week number is set manually (e.g. datList1, datList2,...)
# consists of 7 daily data, 1 week = 7 days
#datList01 = [dat1,dat2,dat3,dat4,dat5,dat6,dat7]

#data35 = read_data('/mnt/eftdata2/pos_ptlf_20131009.txt')
#data35.to_csv('/home/tita/docs/exp2/data35.txt',sep='\t')








