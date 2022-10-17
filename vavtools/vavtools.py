# version 0.0.7 from 17.10.2022
# created by vavabramov

import numpy as np
import pandas as pd
import requests, re, os, io, sys
import time
import threading
import boto3
import math
import multiprocessing as mp
from tqdm import tqdm

__version__ = "0.0.7"

# parallelizer
def parallelize_dataframe(df, func):
    num_processes = mp.cpu_count()
    df_split = np.array_split(df, num_processes)
    with mp.Pool(num_processes) as p:
        df = pd.concat(p.map(func, df_split))
    return df

# preprocessing
def de_punc(text : str) -> str: 
    result = re.sub(r'[^\w\s]', '', text)
    for char in ['_']:
        result = result.replace('_','')
    result = ' '.join(result.split())
    return result

def de_digit(text : str) -> str:
    result = text
    for i in range(10):
        result = result.replace(str(i), '')
    result = ' '.join(result.split())
    return result

def value_fixer(value): # Removes \N values from column & converts to float if possible
    if type(value) is str:
        if '\\N' in value:
            return np.nan
        if isfloat(value):
            return float(value)
    return value

def val_extractor(df: 'DataFrame' = None, text_column : str = None, var_type : str = 'weight') -> 'DataFrame':
    def patterns_creator(vol_vars : 'list of units') -> list:
        base_patterns, patterns = ["(d+vv)", "(d+ vv)", "(d+vv)", "(d+.d+vv)", "(d+.d+ vv)"], []
        for pattern in base_patterns:
            for vv in vol_vars:
                cur_pat = pattern.replace('vv', vv)
                patterns.append(cur_pat.replace('d', '\d').replace('w', '\w'))
        return sorted(patterns, key=len, reverse=True)
    
    def get_raw_val(phrase : str, patterns : list) -> str: 
        text = phrase.lower()
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if len(matches) > 0:
                break
        if len(matches) == 0:
            return np.nan
        return matches[0]
    
    def raw_val_2_num(value : str) -> float:
        if type(value) is not str:
            return value
        number = float(re.sub(r'[^\d\.]', '', value))
        unit = re.sub(r'[\d\.\s]', '', value)
        if unit in ['кг', 'kg', 'л', 'l']:
            number *= 1000
        elif unit in ['мг', 'мгр', 'mg', 'mgr']:
            number /= 1000
        return number
    
    def de_punc_s(text : str) -> str:
        result = re.sub(r'[^\w\s§]', '', text)
        for char in ['_']:
            result = result.replace('_','')
        result = ' '.join(result.split())
        return result
    
    def text_preprocess() -> 'DataFrame':
        df[col] = df[text_column].str.lower().str.strip()
        df[col] = df.apply(lambda row: re.sub(r'(?<=[0-9])(\+|\+ | \+ | \+)(?=[0-9])', ' I ', row[col]), axis = 1)
        df[col] = df.apply(lambda row: re.sub(r'(?<=[0-9])(x|х|x |х | x | х | x| х)(?=[0-9])', ' шт ', row[col]), axis = 1)
        df[col] = df.apply(lambda row: re.sub(r'(?<=[0-9])(\*|\* | \* | \*)(?=[0-9])', ' шт ', row[col]), axis = 1)
        df[col] = df.apply(lambda row: re.sub(r'(?<=[0-9])(,|\.|, |\. | , | \. | ,| \.)(?=[0-9])', '§', row[col]), axis = 1)
        df[col] = df[col].str.replace(r'+', ' ')
        df[col] = df[col].apply(de_punc_s)
        df[col] = df.apply(lambda row: re.sub(r'(?<=[0-9])§(?=[0-9])', '.', row[col]), axis = 1) 
        return df

    col = 'tmp_text_column'
    
    units = {'weight' : ['мгр', 'мг', 'г', 'гр', 'кг', 'kg', 'g', 'gr', 'mgr', 'mg'],
             'volume' : ['мл', 'л', 'ml', 'l'],
             'pieces' : ['шт', 'штук', 'штука', 'штуки', 'уп', 'пак', 'упак', 'pcs']}
    
    def_units = {'weight' : 'gr', 'volume' : 'ml', 'pieces' : 'pcs'}
    
    if var_type not in units.keys():
        raise ValueError("var_type should be 'weight', 'volume' or 'pieces'")
        
    cur_units = units[var_type]
    patterns = patterns_creator(vol_vars = cur_units)
    df = text_preprocess()
    df[var_type+'_'+def_units[var_type]] = df.apply(lambda x: get_raw_val(x[col], patterns), axis = 1)
    df[var_type+'_'+def_units[var_type]] = df[var_type+'_'+def_units[var_type]].apply(raw_val_2_num)
    df = df.drop(columns = [col])
    return df

# type chekers
def isfloat(num) -> bool:
    try:
        float(num)
        return True
    except ValueError:
        return False

# statistics
def get_nan_ratio(df : 'DataFrame') -> 'DataFrame':
    length = df.shape[0]
    ratios = []
    for col in df.columns:
        nan_ratio = 100 * (1 - df[col].count() / length)
        ratios.append(nan_ratio)
    stat_df = pd.DataFrame()
    stat_df['Column'] = df.columns
    stat_df['NaN_ratio_prc'] = ratios
    stat_df['NaN_ratio_prc'] = stat_df['NaN_ratio_prc'].round(2)
    result = stat_df.sort_values(by = ['NaN_ratio_prc'], ascending = False)
    return result

# code utilities
def execution_time(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        res = func(*args, **kwargs)
        t2 = time.time()
        ex_mins = (t2 - t1)//60
        ex_sec =  (t2 - t1)%60
        print(f'Process time: {ex_mins} min, {int(ex_sec)} sec')
        return res
    return wrapper

# OS (files search)
def files_search(directory : 'str', extention : 'str') -> list:
    all_files = os.listdir(directory)    
    requested_files = list(filter(lambda f: f.endswith(extention), all_files))
    return requested_files

#################
###  SQL READ ###
#################
def h_ch_request(query : 'str', ch_user_name : 'str', ch_pwd : 'str', ch_driver_path : 'str') -> 'DataFrame':
    url = 'https://{host}:8443/?database={db}&query={query}'.format(
            host='rc1a-p5ggsxrbfqkfwcu9.mdb.yandexcloud.net',
            db='prod_v2',
            query=query)
    auth = {
            'X-ClickHouse-User': ch_user_name,
            'X-ClickHouse-Key': ch_pwd,
        }
    cacert = ch_driver_path
    urlData = requests.get(url, headers=auth, verify=cacert).content
    rawData = pd.read_csv(io.StringIO(urlData.decode('utf-8')), 
                          sep = '\t',
                          keep_default_na = True, 
                          na_values='\\N',
                          error_bad_lines = False,
                          warn_bad_lines = False)
    return rawData

def h_header_fix(df, new_columns = []):
    values = list(df.columns)
    new_df = pd.DataFrame(columns = new_columns)
    new_df.loc[0] = values
    rename_dict = dict()
    for i in range(len(values)):
        rename_dict[values[i]] = new_columns[i]
    df = df.rename(columns = rename_dict)
    new_df = new_df.append(df)
    return new_df

@execution_time
def get_data(query, column_names, ch_user_name, ch_pwd, ch_driver_path):
    df = h_ch_request(query = query, 
                      ch_user_name = ch_user_name,
                      ch_pwd = ch_pwd, 
                      ch_driver_path = ch_driver_path)
    df = h_header_fix(df, new_columns = column_names)
    for col in df.columns:
        df[col] = df[col].apply(value_fixer)
    return df

####################
###  Excel Write ###
####################
@execution_time
def excel_saver(df, file_name, BS = 10**6):
    writer = pd.ExcelWriter(file_name, options={'strings_to_urls': False})
    number_of_chunks = math.ceil(len(df) / BS)
    chunks = np.array_split(df, number_of_chunks)
    sheet_number = 0
    for chunk in tqdm(chunks):
        chunk.to_excel(writer, sheet_name = f'Sheet{sheet_number}', index = False)
        sheet_number+=1
    writer.save()
    print(f'[OK] - File {file_name} succesfully created')

###############
## S3 Upload ##
###############

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  --> uploading: %.2f%%" % (
                    self._filename,
                    percentage))
            sys.stdout.flush()
            
def s3_upload(file2upload, s3_key_id, s3_key, bucket, s3_directory):
    session = boto3.session.Session()
    s3 = session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=s3_key_id, aws_secret_access_key=s3_key)
                    
    file_name = file2upload.split('/')[-1]
    try:
        s3.upload_file(file2upload, bucket, 
                       f'{s3_directory}/{file_name}', 
                       Callback=ProgressPercentage(file2upload))
        print('\n[OK] - File successfully uploaded!')
    except:
        print('\n[FAIL] - Something went wrong during file upload')

