import pandas as pd
import numpy as np
import pymysql
import random
import pickle
import os
import time

# pickle file 
orstay_pickle_file = 'orstay.pickle'
labevents_pickle_file = 'labevents.pickle'
diagnosis_pickle_file = 'diagnosis.pickle'
vitals_pickle_file = 'vitals.pickle'
ward_vitals_pickle_file = 'ward_vitals.pickle'

# parameter file
vitals_parid_file = 'vital_parid_list.xlsx'
labs_parid_file = 'lab_parid_list.xlsx'
icd9_to_icd10_file = 'icd9toicd10pcs.csv'

# db login
db = pymysql.connect(host='db.anesthesia.co.kr', port=3306, user='vitaldb', passwd='qkdlxkf2469', db='snuop', charset='utf8')
cur = db.cursor()

# measure total time a func executes
def time_trace(func):
    def wrapper(*args, **kwargs):
        st = time.time()
        rt = func(*args, **kwargs)
        print(f'### {func.__name__} time : {time.time()-st:.3f}ms')
        return rt
    return wrapper

# make lab dictionary
def make_lab_dictionary():
    dflcode = pd.read_excel(labs_parid_file, usecols=['lab name', '정규랩', 'POCT'])
    dflcode = dflcode.dropna(subset=['정규랩','POCT'], how='all', axis=0).fillna('')
    dflcode['parid'] = dflcode[['정규랩','POCT']].apply(lambda row: ', '.join(row.values.astype(str)), axis=1)

    dictlcode = dict()
    for idx, row in dflcode.iterrows(): 
        name = row['lab name']
        ids = row['parid'].split(', ')
        for id in ids:
            if id == '':
                continue
            dictlcode[int(id)]=name
        
    return dictlcode

def make_vital_dictionary():
    dfvcode = pd.read_excel(vitals_parid_file).dropna(subset=['inspire'], axis=0)
    return dict(dfvcode[['parid','inspire']].values)

def icd9_to_icd10_code():
    dficd = pd.read_csv(icd9_to_icd10_file).astype(str)
    dficd['icd10cm'] = dficd['icd10cm'].str[:4]  
    dicticd = dict(dficd.drop_duplicates(subset='icd9cm')[['icd9cm','icd10cm']].values) 

    return dicticd

# convert to relative time based on orin
def convert_to_relative_time(orin, dt):
    min = (dt-orin).total_seconds() / 60
    if np.isnan(min):
        return 'x'
    else:
        return round(min/5)*5

# if the data is within the check-in time, 1
def extract_wanted_period(admission_date, recorddate, discharge_date):
    if (admission_date<=recorddate) & (recorddate<=discharge_date):
        return 1
    else:
        return 0   

# preprocess datetime (extract wanted period & convert to relative time)
def preprocess_datetime(dfdata, dfor, starttime, endtime):
    dfmerge = pd.merge(dfdata, dfor[['opid', 'orin', 'subject_id', 'admissiontime','dischargetime']], how='left', on = 'opid') 
    dfmerge['dcnote'] = dfmerge.apply(lambda x: extract_wanted_period(x[starttime], x['charttime'], x[endtime]), axis=1)
    dfresult = dfmerge[dfmerge.dcnote == 1]
    dfresult['charttime'] = dfresult.apply(lambda x: convert_to_relative_time(x['orin'], x['charttime']), axis=1)

    dfresult.drop(['admissiontime','dischargetime', 'dcnote'], axis=1, inplace=True)    
    return dfresult

# make orstay table
def make_orstay_table():
    col_list = ['opid', 'subject_id', 'orin', 'outtime', 'anstarttime', 'anendtime', 'opstarttime', 'opendtime', 'admissiontime', 'dischargetime', 'death_inhosp', 'age', 'gender', 'height', 'weight', 'asa', 'emop', 'department', 'dx', 'opname', 'opcode', 'anesthesia', 'caseid']

    sql = 'SELECT opid, hid, orin, orout, aneinfo_anstart, aneinfo_anend, opstart, opend, admission_date, discharge_date, death_time, ROUND(age,-1), sex, height, weight, premedi_asa, em_yn, dept, diagnosis, opname, replace(icd9_cm_code, ".", ""), aneinfo_anetype, caseid FROM operations WHERE opid > 110101000 and opid < 210101000 and age >= 18 and age <= 90 and anetype != "국소" and orin IS NOT NULL and orout IS NOT NULL and admission_date IS NOT NULL and discharge_date IS NOT NULL and grp = "OR"'

    cur.execute(sql)
    dfor = pd.DataFrame(cur.fetchall(), columns=col_list).fillna('')
    dfor = dfor.astype({'admissiontime':'datetime64[ns]',
                        'dischargetime':'datetime64[ns]',
                        'anstarttime':'datetime64[ns]',
                        'anendtime':'datetime64[ns]',
                        'opstarttime':'datetime64[ns]',
                        'opendtime':'datetime64[ns]',
                        'death_inhosp':'datetime64[ns]'
                        })
    
    # add hadm_hid (random)
    dfhadm = dfor[['subject_id','admissiontime']].drop_duplicates()
    hadmlist = [random.randint(0,100000000) for val in range (0, len(dfhadm))]
    dfhadm['hadm_id'] = hadmlist
    dfor = pd.merge(dfor, dfhadm, how='left', on=['subject_id','admissiontime'])
    
    # replace 9 with 10
    dicticd = icd9_to_icd10_code()
    dfor.replace({'opcode': dicticd}, inplace=True) # orstay 재생성 필요
    
    return dfor

# make labevents table
def make_labevents_table():
    count = 0
    col_list = ['row_id', 'subject_id', 'itemname', 'dt', 'value']
    setlab = set()
    dflab = pd.DataFrame(columns=col_list)
    
    dictlcode = make_lab_dictionary()
    tuplelcode = tuple(dictlcode.keys())
    
    for col in orstay_df[['opid', 'subject_id']].values:
        opid = col[0]
        hid = col[1]
        
        count += 1
        
        cur = db.cursor()
        sql = f'SELECT id,  hid, parid, dt, val FROM labs WHERE hid = {hid} and parid IN {tuplelcode} and val REGEXP "^[0-9]+\\.?[0-9]*$"'
        setlab.add(sql)

        if count % 500 == 0:
            test = ' UNION ALL '.join(list(setlab))
            cur.execute(test)
            labdata = pd.DataFrame(cur.fetchall())  
            dflab = dflab.append(labdata, ignore_index=True)
            setlab = set()
            print(f'add lab data... count: {count}/{len(orstay_df)}')
            
    if count:
        test = ' UNION ALL '.join(list(setlab))
        cur.execute(test)
        labdata = pd.DataFrame(cur.fetchall())  
        dflab = dflab.append(labdata, ignore_index=True)

    dflab.replace({'itemname':dictlcode}, inplace=True)
    return dflab

# make vitals table
def make_vitals_table():
    col_list = ['row_id', 'opid', 'parname', 'orin', 'charttime', 'value']
    count = 0
    setvital = set()
    dfvital = pd.DataFrame(columns=col_list)
    
    dictvcode = make_vital_dictionary()
    tuplevcode = tuple(dictvcode.keys())

    try:
        for opid in orstay_df['opid'].values:
            count += 1
            
            cur = db.cursor()
            sql = f'SELECT id, opid, parid, (SELECT orin FROM operations WHERE opid={opid}), dt, val FROM vitals WHERE opid={opid} and parid IN {tuplevcode} and val REGEXP "^[0-9]+\\.?[0-9]*$"'
            # cur.execute(sql)
            
            # vitaldata = pd.DataFrame(cur.fetchall(), columns=['id', 'opid', 'parid', 'dt', 'val'])  
            # dfvital = dfvital.append(vitaldata, ignore_index=True)
                
            setvital.add(sql)
            
            if count % 500 == 0 :
                test = ' UNION ALL '.join(list(setvital))
                cur.execute(test)
                vitaldata = pd.DataFrame(cur.fetchall())  
                dfvital = dfvital.append(vitaldata, ignore_index=True)
                setvital = set()
                print(f'add vital data... count: {count}/{len(orstay_df)}')
        
        if count:
            test = ' UNION ALL '.join(list(setvital))
            cur.execute(test)
            vitaldata = pd.DataFrame(cur.fetchall())  
            dfvital = dfvital.append(vitaldata, ignore_index=True)
    
    except:
        # vitals convert to relative time
        # dfvital['charttime'] = dfvital.apply(lambda x: convert_to_relative_time(x['orin'], x['charttime']), axis=1)
            
        dfvital.replace({'parname':dictvcode}, inplace=True) 
        return dfvital
    
    # vitals convert to relative time
    # dfvital['charttime'] = dfvital.apply(lambda x: convert_to_relative_time(x['orin'], x['charttime']), axis=1)
        
    dfvital.replace({'parname':dictvcode}, inplace=True) 
    return dfvital

# make ward_vitals table
def make_ward_vitals_table():
    col_list = ['row_id', 'subject_id', 'parname', 'charttime', 'value']
    count = 0
    setwvital = set()
    dfwvital = pd.DataFrame(columns=col_list)
    
    dictvcode = make_vital_dictionary()
    tuplevcode = tuple(dictvcode.keys())

    try:
        for col in orstay_df[['subject_id', 'admissiontime', 'dischargetime']].values:
            hid = col[0]
            admdate = col[1]
            disdate = col[2]
            
            count += 1
            
            cur = db.cursor()
            sql = f'SELECT id, hid, parid, dt, val FROM ward_vitals WHERE dt >= "{admdate}" and dt <= "{disdate}" and hid = {hid} and parid IN {tuplevcode} and val REGEXP "^[0-9]+\\.?[0-9]*$"'
            # cur.execute(sql)
            
            # wvitaldata = pd.DataFrame(cur.fetchall(), columns=['id', 'hid', 'parid', 'dt', 'val'])  
            # dfwvital = dfwvital.append(wvitaldata, ignore_index=True)
            
            setwvital.add(sql)

            if count % 500 == 0:
                test = ' UNION ALL '.join(list(setwvital))
                cur.execute(test)
                wvitaldata = pd.DataFrame(cur.fetchall())  
                dfwvital = dfwvital.append(wvitaldata, ignore_index=True)
                setwvital = set()
                print(f'add ward vital data... count: {count}/{len(orstay_df)}')
                
        if count:
            test = ' UNION ALL '.join(list(setwvital))
            cur.execute(test)
            wvitaldata = pd.DataFrame(cur.fetchall())  
            dfwvital = dfwvital.append(wvitaldata, ignore_index=True)

    except: 
        dfwvital.replace({'parname':dictvcode}, inplace=True) 
        return dfwvital
    
    dfwvital.replace({'parname':dictvcode}, inplace=True)     
    return dfwvital

# make diagnosis table
def make_diagnosis_table():
    dfdgn = pd.read_csv('dataset_diagnosis_total.csv.xz', parse_dates=['진단일자']) # from supreme
    dfdgn = dfdgn[['환자번호','진단일자','ICD10코드']].drop_duplicates()
    dfdgn.columns = ['subject_id', 'charttime', 'icd_code']
    dfdgn['icd_code'] = dfdgn['icd_code'].str[:3] # 진단코드는 앞의 3자리까지만
    
    return dfdgn


if __name__ == '__main__':
        
    # list of tables to create
    listfile = [orstay_pickle_file, labevents_pickle_file, diagnosis_pickle_file, vitals_pickle_file, ward_vitals_pickle_file]

    # create table
    for picfile in listfile:
        item = picfile.rstrip('.pickle')
        if not os.path.exists(picfile):
            print('making...', picfile)
            globals()[item+'_df'] = locals()['make_'+item+'_table']()
            print(globals()[item+'_df'])
            pickle.dump(globals()[item+'_df'], open(picfile, 'wb'))
        else:
            print('using...', picfile)
            globals()[item+'_df'] = pickle.load(open(picfile, 'rb'))

    # orstay convert to relative time
    col_list = ['outtime', 'anstarttime', 'anendtime', 'death_inhosp', 'admissiontime', 'dischargetime', 'opstarttime', 'opendtime']

    for col in col_list:
        orstay_df[col] = orstay_df.apply(lambda x: convert_to_relative_time(x['orin'], x[col]), axis=1)

    # preprocess datetime based on orstay's orin
    dflab_result = preprocess_datetime(labevents_df, orstay_df, admissiontime, dischargetime)
    dfdgn_result = preprocess_datetime(diagnosis_df, orstay_df, admissiontime, dischargetime)
    dfwvital_result = preprocess_datetime(ward_vitals_df, orstay_df, admissiontime, dischargetime)

    dfvital_result = dfwvital_result.append(vitals_df, ignore_index=True)
