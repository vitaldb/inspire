import pandas as pd
import numpy as np
import pymysql
import random
from datetime import datetime
from datetime import timedelta
import pickle
import os
import time

# pickle file
orstay_pickle_file = 'orstay.pickle'
labevents_pickle_file = 'labevents.pickle'
diagnosis_pickle_file = 'diagnosis.pickle'
vitals_pickle_file = 'vitals.pickle'
ward_vitals_pickle_file = 'ward_vitals.pickle'

vitals_parid_filename = 'vital_parid_list.xlsx'

db = pymysql.connect(host='db.anesthesia.co.kr', port=3306, user='vitaldb', passwd='qkdlxkf2469', db='snuop', charset='utf8')
cur = db.cursor()

def time_trace(func):
    def wrapper(*args, **kwargs):
        st = time.time()
        rt = func(*args, **kwargs)
        print(f'### {func.__name__} time : {time.time()-st:.3f}ms')
        return rt
    return wrapper

def make_lab_dictionary():
    dflcode = pd.read_excel('[snuop] lab codes.xlsx', usecols=['lab name', '정규랩', 'POCT'])
    dflcode = dflcode.dropna(subset=['정규랩','POCT'], how='all', axis=0).fillna('')
    dflcode['parid'] = dflcode[['정규랩','POCT']].apply(lambda row: ', '.join(row.values.astype(str)), axis=1)

    dictlcode = dict()
    for col in dflcode[['lab name','parid']].values:
        labname = col[0]
        parids = col[1]
            
        dictlcode[labname] = [id for id in parids.split(', ') if id != '']
        
    return dictlcode
    
# convert to relative time 
def convert_to_relative_time(orin, dt):
    min = (dt-orin).total_seconds() / 60
    if np.isnan(min):
        return 'x'
    else:
        return round(min/5)*5

def extract_wanted_period(admission_date, recorddate, discharge_date):
    if (admission_date<=recorddate) & (recorddate<=discharge_date):
        return 1
    else:
        return 0   

@time_trace
def preprocess_datetime(dfdata, dfor):
    dfmerge = pd.merge(dfdata, dfor[['opid', 'orin', 'hid','admission_date','discharge_date']], how='left', on = 'hid') 
    dfmerge['dcnote'] = dfmerge.apply(lambda x: extract_wanted_period(x['admission_date'], x['dt'], x['discharge_date']), axis=1)
    dfresult = dfmerge[dfmerge.dcnote == 1]
    dfresult['relative_dt'] = dfresult.apply(lambda x: convert_to_relative_time(x['orin'], x['dt']), axis=1)

    return dfresult

# make orstay table
def make_orstay_table():
    col_list = ['opid', 'hid', 'orin', 'orout', 'aneinfo_anstart', 'aneinfo_anend', 'opstart', 'opend', 'admission_date', 'discharge_date', 'death_time', 'sex', 'height', 'weight', 'premedi_asa', 'em_yn', 'dept', 'diagnosis', 'opname', 'opcode', 'aneinfo_anetype']

    sql = 'SELECT opid, hid, orin, orout, aneinfo_anstart, aneinfo_anend, opstart, opend, admission_date, discharge_date, death_time, sex, height, weight, premedi_asa, em_yn, dept, diagnosis, opname, opcode, aneinfo_anetype FROM operations WHERE opid > 110101000 and opid < 210101000 and age >= 18 and age <= 90 and anetype != "국소" and orin IS NOT NULL and orout IS NOT NULL and admission_date IS NOT NULL and discharge_date IS NOT NULL and grp = "OR"'

    cur.execute(sql)
    dfor = pd.DataFrame(cur.fetchall(), columns=col_list)
    dfor = dfor.astype({'admission_date':'datetime64[ns]', 'discharge_date':'datetime64[ns]'})
    
    # add hadm_hid (random)
    dfhadm = dfor[['hid','admission_date']].drop_duplicates()
    hadmlist = [random.randint(0,100000000) for val in range (0, len(dfhadm))]
    dfhadm['hadm_id'] = hadmlist
    dfor = pd.merge(dfor, dfhadm, how='left', on=['hid','admission_date'])
    
    return dfor

# make labevents table
def make_labevents_table():
    count = 0
    setlab = set()
    dflab = pd.DataFrame()
    
    dictlcode = make_lab_dictionary()
    tuplelcode = tuple(sum(dictlcode.values(), [])) 
    
    for col in dfor[['opid', 'hid', 'admission_date', 'discharge_date']].values:
        opid = col[0]
        hid = col[1]
        admdate = col[2]
        disdate = col[3]
        
        count += 1
        
        cur = db.cursor()
        sql = f'SELECT id,  hid, parid, dt, val FROM labs WHERE dt >= "{admdate}" and dt <= "{disdate}" and hid = {hid} and parid IN {tuplelcode} and val REGEXP "^[0-9]+\\.?[0-9]*$"'
        setlab.add(sql)

        if count % 500 == 0:
            test = ' UNION ALL '.join(list(setlab))
            cur.execute(test)
            labdata = pd.DataFrame(cur.fetchall(), columns=['id', 'hid', 'parid', 'dt', 'val'])  
            dflab = dflab.append(labdata, ignore_index=True)
            setlab = set()
            print(f'add lab data... count: {count}/{len(dfor)}')
            
    if count:
        test = ' UNION ALL '.join(list(setlab))
        cur.execute(test)
        labdata = pd.DataFrame(cur.fetchall(), columns=['id', 'hid', 'parid', 'dt', 'val'])  
        dflab = dflab.append(labdata, ignore_index=True)

    return dflab

# make vitals table
@time_trace
def make_vitals_table():
    count = 0
    setvital = set()
    dfvital = pd.DataFrame()
    
    dfvcode = pd.read_excel(vitals_parid_filename).dropna(subset=['inspire'], axis=0)
    tuplevcode = tuple(dfvcode['parid'].values) 

    for opid in dfor['opid'].values:
        count += 1
        
        cur = db.cursor()
        sql = f'SELECT id, opid, parid, dt, val FROM vitals WHERE opid={opid} and parid IN {tuplevcode} and val REGEXP "^[0-9]+\\.?[0-9]*$"'
        cur.execute(sql)
        
        vitaldata = pd.DataFrame(cur.fetchall(), columns=['id', 'opid', 'parid', 'dt', 'val'])  
        dfvital = dfvital.append(vitaldata, ignore_index=True)
            
        if count % 100 == 0 :
            print(f'add vital data... count: {count}/{len(dfor)}')
    #     setvital.add(sql)
        
    #     if count % 100 == 0 :
    #         test = ' UNION ALL '.join(list(setvital))
    #         cur.execute(test)
    #         vitaldata = pd.DataFrame(cur.fetchall(), columns=['id', 'opid', 'parid', 'dt', 'val'])  
    #         dfvital = dfvital.append(vitaldata, ignore_index=True)
    #         setvital = set()
    #         print(f'add vital data... count: {count}/{len(dfor)}')
    
    # if count:
    #     test = ' UNION ALL '.join(list(setvital))
    #     cur.execute(test)
    #     vitaldata = pd.DataFrame(cur.fetchall(), columns=['id', 'opid', 'parid', 'dt', 'val'])  
    #     dfvital = dfvital.append(vitaldata, ignore_index=True)

    return dfvital

# make ward_vitals table
@time_trace
def make_ward_vitals_table():
    count = 0
    setwvital = set()
    dfwvital = pd.DataFrame()
    
    dfvcode = pd.read_excel(vitals_parid_filename).dropna(subset=['inspire'], axis=0)
    tuplevcode = tuple(dfvcode['parid'].values)
     
    for col in dfor[['hid', 'admission_date', 'discharge_date']].values:
        hid = col[0]
        admdate = col[1]
        disdate = col[2]
        
        count += 1
        
        cur = db.cursor()
        sql = f'SELECT id, hid, parid, dt, val FROM ward_vitals WHERE dt >= "{admdate}" and dt <= "{disdate}" and hid = {hid} and parid IN {tuplevcode} and val REGEXP "^[0-9]+\\.?[0-9]*$"'
        cur.execute(sql)
        
        wvitaldata = pd.DataFrame(cur.fetchall(), columns=['id', 'hid', 'parid', 'dt', 'val'])  
        dfwvital = dfwvital.append(wvitaldata, ignore_index=True)
        
        if count % 500 == 0:
            print(f'add ward vital data... count: {count}/{len(dfor)}')

    #     setwvital.add(sql)

    #     if count % 500 == 0:
    #         test = ' UNION ALL '.join(list(setwvital))
    #         cur.execute(test)
    #         wvitaldata = pd.DataFrame(cur.fetchall(), columns=['id', 'hid', 'parid', 'dt', 'val'])  
    #         dfwvital = dfwvital.append(wvitaldata, ignore_index=True)
    #         setwvital = set()
    #         print(f'add ward vital data... count: {count}/{len(dfor)}')
            
    # if count:
    #     test = ' UNION ALL '.join(list(setwvital))
    #     cur.execute(test)
    #     wvitaldata = pd.DataFrame(cur.fetchall(), columns=['id', 'hid', 'parid', 'dt', 'val'])  
    #     dfwvital = dfwvital.append(wvitaldata, ignore_index=True)
        
    return dfwvital


# make diagnosis table
def make_diagnosis_table():
    dfdgn = pd.read_csv('dataset_diagnosis_total.csv.xz', parse_dates=['진단일자'])
    dfdgn = dfdgn[['환자번호','진단일자','ICD10코드','ICD10명']].drop_duplicates()
    dfdgn.columns = ['hid', 'dt', 'ICD10CODE', 'ICD10NAME']
    
    return dfdgn

if not os.path.exists(orstay_pickle_file):
    dfor = make_orstay_table()  
    pickle.dump(dfor, open(orstay_pickle_file, 'wb'))  
else:
    print('using',orstay_pickle_file)
    dfor = pickle.load(open(orstay_pickle_file, 'rb'))

if not os.path.exists(labevents_pickle_file):
    dflab = make_labevents_table()  
    pickle.dump(dflab, open(labevents_pickle_file, 'wb'))  
else:
    print('using',labevents_pickle_file)
    dflab = pickle.load(open(labevents_pickle_file, 'rb'))

if not os.path.exists(diagnosis_pickle_file):
    dfdgn = make_diagnosis_table()
    pickle.dump(dfdgn, open(diagnosis_pickle_file, 'wb'))
else:
    print('using',diagnosis_pickle_file)
    dfdgn = pickle.load(open(diagnosis_pickle_file, 'rb'))

if not os.path.exists(ward_vitals_pickle_file):
    print('making... ward vital data')
    dfwvital = make_ward_vitals_table()
    pickle.dump(dfwvital, open(ward_vitals_pickle_file, 'wb'))
else:
    print('using',ward_vitals_pickle_file)
    dfwvital = pickle.load(open(ward_vitals_pickle_file, 'rb'))

if not os.path.exists(vitals_pickle_file):
    print('making... vital data')
    dfvital = make_vitals_table()
    pickle.dump(dfvital, open(vitals_pickle_file, 'wb'))
else:
    print('using',vitals_pickle_file)
    dfvital = pickle.load(open(vitals_pickle_file, 'rb'))

# # orstay convert to relative time
# dfor['relative_orout'] = dfor.apply(lambda x: convert_to_relative_time(x['orin'], x['orout']), axis=1)
# dfor['relative_anstart'] = dfor.apply(lambda x: convert_to_relative_time(x['orin'], x['aneinfo_anstart']), axis=1)
# dfor['relative_anend'] = dfor.apply(lambda x: convert_to_relative_time(x['orin'], x['aneinfo_anend']), axis=1)
# dfor['relative_death'] = dfor.apply(lambda x: convert_to_relative_time(x['orin'], x['death_time']), axis=1)

# dfor['relative_adm'] = dfor.apply(lambda x: convert_to_relative_time(x['orin'], x['admission_date']), axis=1)
# dfor['relative_dsch'] = dfor.apply(lambda x: convert_to_relative_time(x['orin'], x['discharge_date']), axis=1)

dflab_result = preprocess_datetime(dflab, dfor)
dfdgn_result = preprocess_datetime(dfdgn, dfor)
dfwvital_result = preprocess_datetime(dfwvital, dfor)