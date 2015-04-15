from IPython.parallel import Client
import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO
import pandas as pd
import numpy as np
import re


def read_data(path):

    import sys
    if sys.version_info[0] < 3:
        from StringIO import StringIO
    else:
        from io import StringIO
   

    headclm = ['post_date', 'acq_fiid', 'term_id', 'aud_no', 'msg_type','card_no','exp_date','trk2','card_ln','card_fiid','acq_ln','tran_cde','pos_entry_mde','pos_cond_cde','pin_ind','sic','responder','rc','draft_capt_flg','amt1','amt2','b24_acq_date','b24_acq_time','acq_ic_setl_dat','iss_ic_setl_dat','retailer_id','head_term_tim','head_rec_type','auth_term_city','auth_term_cntry_cde','p0_merch_num','p0_fbck_txn','p0_chip_cond_cde','p0_trk2_len','p0_term_class','p0_emv_enabled_term','p0_pt_srv_entry_mde','pb_tran_cc','pb_auth_rc','b3_term_cap','b3_emv_term_type','b4_pt_srv_entry_mde','b4_term_entry_cap','b4_last_emv_stat','b4_data_suspect','b4_dev_info','b4_arqc_vrfy','c0_e_com_flg','c0_cci','c0_order_type','c0_tran_type','c0_sli','c0_cct','c0_term_postal_cde','card_id']
    
    
    cntLn = 0
    arrLine = ""
    strLine = ""
    strLine2 = ""
    strLine3 = ""
    fileOpen = open(path, 'r')

    for line in fileOpen:
        arrLine = line 
        arrLine = arrLine.replace('\'','')
        arrLine = arrLine.replace('\"','')
    	cntLn = cntLn + 1
        if (cntLn <= 1000000):
            strLine = strLine + arrLine 
        elif (cntLn > 1000000 and cntLn <= 3000000):
            strLine2 = strLine2 + arrLine
        else:
            strLine3 = strLine3 + arrLine
        arrLine = ""
        
    dat1 = pd.read_table(StringIO(strLine),sep=',',index_col=None,header=None,names=headclm, error_bad_lines=False, dtype = unicode);
    dat2 = pd.read_table(StringIO(strLine2),sep=',',index_col=None,header=None,names=headclm, error_bad_lines=False, dtype = unicode);
    dat3 = pd.read_table(StringIO(strLine3),sep=',',index_col=None,header=None,names=headclm, error_bad_lines=False, dtype = unicode);
	
    data1 = dat1[1:len(dat1)]
    data2 = dat2[1:len(dat2)]
    data3 = dat3[1:len(dat3)]
    arrData = [data1,data2,data3]
    dataall = pd.concat(arrData,ignore_index=True)

    return dataall


def clean_data(dataall):

    cleaned = dataall[dataall['retailer_id'].str.len()!=0]
    datasample=cleaned[['card_no','acq_fiid', 'card_fiid','tran_cde','sic','amt1','amt2','b24_acq_time','retailer_id']]
    datasample['strcard_no']=map(str,datasample['card_no'])
    datasample['strtran_cde']=map(str,datasample['tran_cde'])
    datasample['strsic']=map(str,datasample['sic'])
    datasample['amt1']=datasample['amt1'].convert_objects(convert_numeric = True)
    datasample['amt1']=datasample['amt1'].fillna(0.0)
    datasample['famt1']=datasample['amt1'].astype(float)
    datasample['amt2']=datasample['amt2'].convert_objects(convert_numeric = True)
    datasample['amt2']=datasample['amt2'].fillna(0.0)
    datasample['famt2']=datasample['amt2'].astype(float)
    datasample['strcard_fiid']=map(str,datasample['card_fiid'])
    datasample['stracq_fiid']=map(str,datasample['acq_fiid'])
    datasample['strretailer_id']=map(str,datasample['retailer_id'])
    datasample['strtime']=map(str,datasample['b24_acq_time'])
    datasample['str_cardno'] = [cardno.strip() for cardno in datasample['strcard_no']]
    datasample['str_trancde'] = [trancde.strip() for trancde in datasample['strtran_cde']]
    datasample['sicstr'] = [sic.strip() for sic in datasample['strsic']]
    datasample['timestr'] = [timestr.strip() for timestr in datasample['strtime']]
    datasample['str_retailerid'] = [rid.strip() for rid in datasample['strretailer_id']]
    datasample['str_cardfiid'] = [cardfiid.strip() for cardfiid in datasample['strcard_fiid']]
    datasample['str_acqfiid'] = [acqfiid.strip() for acqfiid in datasample['stracq_fiid']]
    
    cardno = None
    datasample['strcardno'] = ["".join(cardno.split()) for cardno in datasample['str_cardno']]
    trancde = None
    datasample['strtrancde'] = ["".join(trancde.split()) for trancde in datasample['str_trancde']]
    sic = None
    datasample['str_sic'] = ["".join(sic.split()) for sic in datasample['sicstr']]
    timestr = None
    datasample['str_time'] = ["".join(timestr.split()) for timestr in datasample['timestr']]
    rid = None
    datasample['strretailerid'] = ["".join(rid.split()) for rid in datasample['str_retailerid']]
    cardfiid = None
    datasample['strcardfiid'] = ["".join(cardfiid.split()) for cardfiid in datasample['str_cardfiid']]
    acqfiid = None
    datasample['stracqfiid'] = ["".join(acqfiid.split()) for acqfiid in datasample['str_acqfiid']]
    retailerdat=datasample[['strretailerid','str_time','str_sic','stracqfiid','strcardfiid','strcardno','strtrancde','famt1','famt2']]

    datasample=[]
    datasample=None

    retailer=retailerdat[(retailerdat['strtrancde'].str.len()==6)]
    trancde = None
    retailer['tc']=[trancde[:2] for trancde in retailer['strtrancde']]
    trancde = None
    retailer['t']=[trancde[2] for trancde in retailer['strtrancde']]
    trancde = None
    retailer['aa']=[trancde[3:5] for trancde in retailer['strtrancde']]
    trancde = None
    retailer['c']=[trancde[5] for trancde in retailer['strtrancde']]

    retailerdat=[]
    retailerdat=None
    data=retailer[(retailer['strretailerid'].str.len()!=0)]
    
    return data
    

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
c[:].run('prep.py')
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








