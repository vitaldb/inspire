import pandas as pd
import numpy as np
import pymysql
import random
import os
import pickle
from datetime import timedelta
import random
import warnings
warnings.filterwarnings('ignore')

# db login
db = pymysql.connect(host='db.anesthesia.co.kr', port=3306, user='vitaldb', passwd='qkdlxkf2469', db='snuop', charset='utf8')
cur = db.cursor()

# table pickle file 
operations_pickle_file = 'operations.pickle'
labevents_pickle_file = 'labevents.pickle'
diagnosis_pickle_file = 'diagnosis.pickle'
vitals_pickle_file = 'vitals.pickle'
ward_vitals_pickle_file = 'ward_vitals.pickle'

# parameter file
vitals_parid_file = 'vital_parid_list.xlsx'
labs_parid_file = 'lab_parid_list.xlsx'
icd9_to_icd10_file = 'icd9toicd10pcs.csv'

# deidentified mapping file
opid_mapping_file = 'opid_mapping.csv'
hid_mapping_file = 'hid_mapping.csv'
hadm_mapping_file = 'hadm_mapping.csv'

# dtstart, dtend based on opdate
DTSTART = '2011-01-01'
DTEND = '2011-01-31'

# opid using DTSTART, DTEND
dtstart =  int(DTSTART.replace('-','').lstrip('20'))*1000
dtend = int(DTEND.replace('-','').lstrip('20'))*1000 + 1000

# make lab parameters dictionary
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

# make vital parameters dictionary
def make_vital_dictionary():
    dfvcode = pd.read_excel(vitals_parid_file).dropna(subset=['inspire'], axis=0)
    return dict(dfvcode[['parid','inspire']].values)

# replace icd9 to icd10
def icd9_to_icd10_code():
    dficd = pd.read_csv(icd9_to_icd10_file).astype(str)
    dficd['icd10cm'] = dficd['icd10cm'].str[:4]
    dicticd = dict(dficd.drop_duplicates(subset='icd9cm')[['icd9cm','icd10cm']].values) 
    
    return dicticd

# replace anetype name
def replace_anetype(ane):
    if ane == 'General':
        return 'General'
    elif ane == 'Spinal':
        return 'Spinal'
    elif ane == 'Epidural':
        return 'Epidural'
    elif ane == 'Combined (S,E)':
        return 'Combined'
    elif ane == 'MAC':
        return 'MAC'
    else:
        return ''

# replace float to int
def replace_int(row):
    try:
        return(int(row))
    except:
        return(0)

# if the data is within the check-in time, 1
def extract_wanted_period(admission_date, recorddate, discharge_date):
    if (admission_date<=recorddate) & (recorddate<=discharge_date):
        return 1
    else:
        return 0   

# convert to relative time based on orin
def convert_to_relative_time(orin, dt):
    min = (dt-orin).total_seconds() / 60
    if np.isnan(min):
        return ''
    else:
        return round(min/5)*5

# remove data other than real numbers
def extract_float(x):
    try: 
       x = float(x)
       return 1
    except:
        return 0 
    
def deidentified_data(dataframe):
    # deidentified opid
    mapping_opid_df = pd.read_csv(opid_mapping_file, dtype={'opid':float})
    mapping_opid_dict = dict(mapping_opid_df.values)
    dataframe['opid'] = dataframe['opid'].map(mapping_opid_dict).astype(int)
    
    # deidentified hid
    if 'subject_id' in dataframe.columns:
        try:
            mapping_hid_df = pd.read_csv(hid_mapping_file) 
        except:
            mapping_hid_df = pd.DataFrame(columns=['hid', 'dehid'])

        dfresult = dataframe.copy()
        for hid in dataframe['subject_id'].values: 
            if hid in mapping_hid_df['hid'].values: # if subject_id already exists, get from mapping list
                dehid = mapping_hid_df.loc[mapping_hid_df['hid']==hid, 'dehid'].values[0]
                dfresult.loc[dfresult['subject_id']==hid, 'subject_id'] = dehid
                continue
            else:
                dehid = random.randint(1, 10**8) + 1*10**8
                while dehid in mapping_hid_df['dehid'].values: 
                    dehid = random.randint(10**8) + 1*10**8
                    if dehid not in mapping_hid_df['dehid'].values: # dehid must not be in the mapping list
                        mapping_hid_df = mapping_hid_df.append({'hid':hid, 'dehid':dehid}, ignore_index=True)
                        dfresult.loc[dfresult['subject_id']==hid, 'subject_id'] = dehid        
                        break
                else:
                    mapping_hid_df = mapping_hid_df.append({'hid':hid, 'dehid':dehid}, ignore_index=True)
                    dfresult.loc[dfresult['subject_id']==hid, 'subject_id'] = dehid        
                        
        mapping_hid_df.to_csv(hid_mapping_file, index=False)
    else:
        dfresult = dataframe
    
    # deidentified hadm_id
    if 'hadm_id' in dfresult.columns:
        dfhadm = dfresult[['subject_id','admissiontime']].drop_duplicates()
        try: 
            mapping_hadm_df = pd.read_csv(hadm_mapping_file, parse_dates=['admission_date'])
        except:
            mapping_hadm_df = pd.DataFrame(columns=['dehid','admission_date', 'hid_adm', 'deadm'])
            
        dfresult2 = dfresult.copy()
        for col in dfhadm[['subject_id','admissiontime']].values:
            hid = col[0]
            adm = col[1]
            
            hid_adm = str(hid) + str(adm) # combination of hid and adm is on pair
            
            if hid_adm in mapping_hadm_df['hid_adm'].values:  # if hid_adm already exists, get from mapping list
                deadm = mapping_hadm_df.loc[mapping_hadm_df['hid_adm']==hid_adm, 'deadm'].values[0]
                dfresult2.loc[(dfresult2['subject_id']==hid) & (dfresult2['admissiontime']==adm), 'hadm_id'] = deadm     
                continue            
            else:
                deadm = random.randint(1, 10**8) + 2*10**8
                while deadm in mapping_hadm_df['deadm'].values:
                    deadm = random.randint(10**8) + 2*10**8
                    if deadm not in mapping_hadm_df['deadm'].values: # deadm must not be in the mapping list
                        mapping_hadm_df = mapping_hadm_df.append({'dehid':hid, 'admission_date':adm, 'hid_adm':hid_adm, 'deadm':deadm}, ignore_index=True)
                        dfresult2.loc[(dfresult2['subject_id']==hid) & (dfresult2['admissiontime']==adm), 'hadm_id'] = deadm           
                        break
                else:
                    mapping_hadm_df = mapping_hadm_df.append({'dehid':hid, 'admission_date':adm, 'hid_adm':hid_adm, 'deadm':deadm}, ignore_index=True)
                    dfresult2.loc[(dfresult2['subject_id']==hid) & (dfresult2['admissiontime']==adm), 'hadm_id'] = deadm     
        
        mapping_hadm_df.to_csv(hadm_mapping_file, index=False)
    else:
        dfresult2 = dfresult    
        
                
    return dfresult2

# make operations table
def make_operations_table(dtstart, dtend):
    col_list = ['opid', 'subject_id', 'orin', 'outtime', 'anstarttime', 'anendtime', 'opstarttime', 'opendtime', 'admissiontime', 'dischargetime', 'deathtime_inhosp', 'age', 'gender', 'height', 'weight', 'asa', 'emop', 'department', 'icd10_pcs', 'anetype', 'caseid']

    sql = f'SELECT opid, hid, orin, orout, aneinfo_anstart, aneinfo_anend, opstart, opend, admission_date, discharge_date, death_time, ROUND(age,-1), sex, ROUND(height), ROUND(weight), premedi_asa, em_yn, dept, replace(icd9_cm_code, ".", ""), aneinfo_anetype, caseid FROM operations WHERE opid > {dtstart} and opid < {dtend} and age >= 18 and age <= 90 and anetype != "국소" and orin IS NOT NULL and orout IS NOT NULL and admission_date IS NOT NULL and discharge_date IS NOT NULL and grp = "OR"'

    cur.execute(sql)
    dfor = pd.DataFrame(cur.fetchall(), columns=col_list).fillna('')
    dfor = dfor.astype({'admissiontime':'datetime64[ns]',
                        'dischargetime':'datetime64[ns]',
                        'anstarttime':'datetime64[ns]',
                        'anendtime':'datetime64[ns]',
                        'opstarttime':'datetime64[ns]',
                        'opendtime':'datetime64[ns]',
                        'deathtime_inhosp':'datetime64[ns]'
                        })
    
    # add column : hadm_hid
    dfor['hadm_id'] = '' 
    
    # replace 9 with 10
    dicticd = icd9_to_icd10_code()
    dfor['icd10_pcs'] = dfor['icd10_pcs'].map(dicticd)
    
    # replace anetype title
    dfor['anetype'] = dfor.apply(lambda x: replace_anetype(x['anetype']), axis=1)
    
    # convert real number to integer
    dfor['age'] = dfor.apply(lambda x : replace_int(x['age']), axis=1)
    dfor['height'] = dfor.apply(lambda x : replace_int(x['height']), axis=1)
    dfor['weight'] = dfor.apply(lambda x : replace_int(x['weight']), axis=1)
    
    return dfor

# make labevents table
def make_labevents_table(dfor):
    cur = db.cursor()
    sql = f'SELECT * FROM labs WHERE dt >= DATE_SUB("{DTSTART}", INTERVAL 1 MONTH) and dt <= DATE_ADD("{DTEND}", INTERVAL 1 MONTH)'
    cur.execute(sql)
    data = cur.fetchall()
    
    col_list = ['row_id', 'subject_id', 'itemname', 'charttime', 'value']
    dflab = pd.DataFrame(data, columns=col_list)
    
    dictlcode = make_lab_dictionary()
    dflab.replace({'itemname': dictlcode}, inplace=True)
    dflab = dflab[dflab['itemname'].str.contains('|'.join(list(set(dictlcode.values()))), na=False)] # remove all lab not in the lab dictionary
    
    dflab['float'] = dflab['value'].apply(extract_float) # remove all not real values
    dflab = dflab[dflab['float']==1] 
    
    dfmerge = pd.merge(dflab, dfor[['opid', 'orin', 'subject_id', 'admissiontime','dischargetime']], how='left', on = 'subject_id')
    dfmerge['dcnote'] = dfmerge.apply(lambda x: extract_wanted_period(x['orin']-timedelta(days=30), x['charttime'], x['orin']+timedelta(days=30)), axis=1) # only lab data within 1 month from orin
    dfresult = dfmerge[dfmerge.dcnote == 1]
    dfresult['charttime'] = dfresult.apply(lambda x: convert_to_relative_time(x['orin'], x['charttime']), axis=1) # relative time based on orin
    dfresult.drop(['admissiontime','dischargetime', 'dcnote', 'orin', 'float'], axis=1, inplace=True)    
        
    return dfresult

def make_vitals_table(dfor, dtstart, dtend):
    cur = db.cursor()
    sql = f'SELECT * FROM vitals WHERE opid > {dtstart} and opid < {dtend}'
    cur.execute(sql)
    data = cur.fetchall()
    
    col_list = ['opid', 'itemname', 'charttime', 'value', 'row_id']
    dfvital = pd.DataFrame(data, columns=col_list)
    
    dictvcode = make_vital_dictionary()
    dfvital.replace({'itemname': dictvcode}, inplace=True)
    dfvital = dfvital[dfvital['itemname'].str.contains('|'.join(list(set(dictvcode.values()))), na=False)] # remove all vital not in the vital dictionary
    
    dfvital['float'] = dfvital['value'].apply(extract_float)
    dfvital = dfvital[dfvital['float']==1] # remove all not real values
    
    dfmerge = pd.merge(dfvital, dfor[['opid', 'orin']], how='left', on = 'opid')
    dfmerge['charttime'] = dfmerge.apply(lambda x: convert_to_relative_time(x['orin'], x['charttime']), axis=1) # relative time based on orin
    dfmerge = dfmerge[~(dfmerge['charttime']=='x')]
    dfmerge.drop(['row_id', 'orin', 'float'], axis=1, inplace=True)
    
    return dfmerge
    
def make_ward_vitals_table(dfor):
    tuplehid = tuple(dfor['subject_id'].unique())
    
    cur = db.cursor()
    sql = f'SELECT * FROM ward_vitals WHERE hid IN {tuplehid}'
    cur.execute(sql)
    data = cur.fetchall()
    
    col_list = ['row_id', 'subject_id', 'itemname', 'charttime', 'value']
    dfwvital = pd.DataFrame(data, columns=col_list)
    
    dictvcode = make_vital_dictionary()
    dfwvital.replace({'itemname': dictvcode}, inplace=True)
    dfwvital = dfwvital[dfwvital['itemname'].str.contains('|'.join(list(set(dictvcode.values()))), na=False)] # remove all vital not in the vital dictionary
    
    dfwvital['float'] = dfwvital['value'].apply(extract_float) 
    dfwvital = dfwvital[dfwvital['float']==1] # remove all not real values
    
    dfmerge = pd.merge(dfwvital, dfor[['opid', 'orin', 'subject_id', 'admissiontime','dischargetime']], how='left', on = 'subject_id')
    dfmerge['dcnote'] = dfmerge.apply(lambda x: extract_wanted_period(x['admissiontime'], x['charttime'], x['dischargetime']), axis=1) # only ward vital data within admission time and discharge time
    dfresult = dfmerge[dfmerge.dcnote == 1]
    dfresult['charttime'] = dfresult.apply(lambda x: convert_to_relative_time(x['orin'], x['charttime']), axis=1) # relative time based on orin
    dfresult.drop(['row_id', 'subject_id', 'admissiontime', 'dischargetime', 'dcnote', 'orin', 'float'], axis=1, inplace=True) 
    
    return dfresult

# make diagnosis table
def make_diagnosis_table(dfor):
    dfdgn = pd.read_csv('dataset_diagnosis_total.csv.xz', parse_dates=['진단일자']) # from supreme
    dfdgn = dfdgn[['환자번호','진단일자','ICD10코드']].drop_duplicates()
    dfdgn.columns = ['subject_id', 'charttime', 'icd_code']
    dfdgn['icd_code'] = dfdgn['icd_code'].str[:3] # icd code is up to the first 3 letters only

    dfmerge = pd.merge(dfdgn, dfor[['opid', 'orin', 'subject_id', 'admissiontime','dischargetime']], how='left', on = 'subject_id') 
    dfmerge['dcnote'] = dfmerge.apply(lambda x: extract_wanted_period(x['admissiontime'], x['charttime'], x['dischargetime']), axis=1) # only diagnosis data within admission time and discharge time
    dfresult = dfmerge[dfmerge.dcnote == 1]
    dfresult['charttime'] = dfresult.apply(lambda x: convert_to_relative_time(x['orin'], x['charttime']), axis=1) # relative time based on orin

    dfresult.drop(['orin', 'admissiontime','dischargetime', 'dcnote'], axis=1, inplace=True)    
        
    return dfresult

def make_vital_labeling():
    # 1. cpb on/off labeling
    path = 'cpb_time_data.xlsx'
    dfcpb = pd.read_excel(path, usecols= ['환자번호','서식작성일','진료서식구성원소ID','서식내용'], skiprows=1, parse_dates=['서식작성일'])

    cpbmax = dfcpb.groupby(['환자번호','서식작성일'], as_index=False)['진료서식구성원소ID'].max() 
    cpbmin = dfcpb.groupby(['환자번호','서식작성일'], as_index=False)['진료서식구성원소ID'].min() 
    
    dfmax = pd.merge(cpbmax, dfcpb, on = ['환자번호','서식작성일','진료서식구성원소ID']) 
    dfmin = pd.merge(cpbmin, dfcpb, on = ['환자번호','서식작성일','진료서식구성원소ID']) 
    
    dfmax['서식항목명'] = 'cpb off'
    dfmin['서식항목명'] = 'cpb on'
    
    dfcpb = dfmax.append(dfmin, ignore_index=True).sort_values(['서식작성일'])
    
    operations_df['opdate'] = operations_df['orin'].dt.date
    
    cpbopid = pd.merge(dfcpb, operations_df[['opid','subject_id','opdate','orin']], how='left',  left_on=['환자번호','dt'], right_on=['subject_id','opdate'])
    
    test = cpbopid[cpbopid['opdate'].notnull()]
    
    test['서식내용'] = test['서식내용'].replace('/', ':', regex=True).replace('24:', '0:', regex=True)
    
    dfresult = pd.DataFrame(columns=['opid','charttime','itemname','value'])
    for col in test[['opid','orin']].drop_duplicates().values  :
        timedict = dict()
        opid = col[0]
        orin = col[1]
        
        data = test[test['opid']==opid]
        data['dt'] = pd.to_datetime(data['orin'].dt.date.astype(str)  + ' ' + data['서식내용'])
        
        timedict['orin'] = orin
        
        timedict.update(dict(data[['서식항목명','dt']].values))
        
        if timedict['cpb on'] < timedict['orin']:
            timedict['cpb on'] = timedict['cpb on'] + timedelta(days=1)
        if timedict['cpb off'] < timedict['orin']:
            timedict['cpb off'] = timedict['cpb off'] + timedelta(days=1)
        
        test2 = pd.DataFrame.from_records([timedict], index=['time'])
    
        test2['cpb on'] = test2.apply(lambda x: convert_to_relative_time(x['orin'], x['cpb on']), axis=1)
        test2['cpb off'] = test2.apply(lambda x: convert_to_relative_time(x['orin'], x['cpb off']), axis=1)
        
        result = pd.DataFrame(range(test2['cpb on'].values[0], test2['cpb off'].values[0]+1,5), columns=['charttime']) 
        
        result[['opid', 'itemname', 'value']]=[opid, 'cpb', 1]
        
        dfresult = dfresult.append(result, ignore_index=True)
        print(dfresult)        
    
    # 2. crrt, ecmo
    # sql = "SELECT DISTINCT(hid) FROM `admissions` where icuin > '2019-01-01' and icuout < '2021-12-31'"
        
if not os.path.exists(operations_pickle_file):
    print('making...', operations_pickle_file)
    operations_df = make_operations_table(dtstart, dtend)
    pickle.dump(operations_df, open(operations_pickle_file, 'wb'))
else: 
    print('using...', operations_pickle_file)
    operations_df = pickle.load(open(operations_pickle_file, 'rb'))
    
if not os.path.exists(diagnosis_pickle_file):
    print('making...', diagnosis_pickle_file)
    diagnosis_df = make_diagnosis_table(operations_df)
    pickle.dump(diagnosis_df, open(diagnosis_pickle_file, 'wb'))
else: 
    print('using...', diagnosis_pickle_file)
    diagnosis_df = pickle.load(open(diagnosis_pickle_file, 'rb'))

if not os.path.exists(labevents_pickle_file):
    print('making...', labevents_pickle_file)
    labevents_df = make_labevents_table(operations_df)
    pickle.dump(labevents_df, open(labevents_pickle_file, 'wb'))
else: 
    print('using...', labevents_pickle_file)
    labevents_df = pickle.load(open(labevents_pickle_file, 'rb'))

if not os.path.exists(vitals_pickle_file):
    print('making...', vitals_pickle_file)
    vitals_df = make_vitals_table(operations_df, dtstart, dtend)
    pickle.dump(vitals_df, open(vitals_pickle_file, 'wb'))
else: 
    print('using...', vitals_pickle_file)
    vitals_df = pickle.load(open(vitals_pickle_file, 'rb'))
    
if not os.path.exists(ward_vitals_pickle_file):
    print('making...', ward_vitals_pickle_file)
    ward_vitals_df = make_ward_vitals_table(operations_df)
    pickle.dump(ward_vitals_df, open(ward_vitals_pickle_file, 'wb'))
else: 
    print('using...', ward_vitals_pickle_file)
    ward_vitals_df = pickle.load(open(ward_vitals_pickle_file, 'rb'))

# merge ward_vitals & vitals 
vitals_df = vitals_df.append(ward_vitals_df, ignore_index=True)
vitals_df = vitals_df.astype({'value':float})
vitals_df = vitals_df.groupby(['opid', 'charttime', 'itemname'], as_index=False).median()

# # deidentified data
operations_df = deidentified_data(operations_df)
vitals_df = deidentified_data(vitals_df)
labevents_df = deidentified_data(labevents_df)
diagnosis_df = deidentified_data(diagnosis_df)

# operations_df replace time
convert_col_list = ['outtime','anstarttime','anendtime','opstarttime','opendtime','admissiontime','dischargetime','deathtime_inhosp']
for col in convert_col_list:
    operations_df[col] = operations_df.apply(lambda x: convert_to_relative_time(x['orin'], x[col]), axis=1) 
operations_df.drop(['orin'], axis=1, inplace=True)

# save test file
operations_df.to_csv('201101_operations_test.csv', index=False, encoding='utf-8-sig')
vitals_df.to_csv('201101_vitals_test.csv', index=False, encoding='utf-8-sig')
labevents_df.to_csv('201101_labevents_test.csv', index=False, encoding='utf-8-sig')
diagnosis_df.to_csv('201101_diagnosis_test.csv', index=False, encoding='utf-8-sig')
