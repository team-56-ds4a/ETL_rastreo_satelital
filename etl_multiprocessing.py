import pandas as pd
import os
import time
from multiprocessing import Pool
from services import sql_service
from utils import delete_duplicated_columns
import re

def read_csv_mp(filename):
    reg_exp_veh = "[a-zA-Z]{3}[0-9]{3}|[a-zA-Z]{3}[0-9]{2}[a-zA-Z]"
    patron  = re.compile(reg_exp_veh)
    df_day = pd.DataFrame()
    try:
        data = pd.read_excel(filename,sheet_name=None)
        for name, column in data.items():
            if patron.search(name):   
                column.columns = map(lambda x: str(x).upper(), column.columns)
                column.columns = column.columns.str.replace(' ', '_')
                column = delete_duplicated_columns(column)
                column= column.drop(['#'], axis=1)
                column['PLACA'] = patron.search(name).group()
                column = column.reset_index(drop=True)
                try :
                    df_day = pd.concat([df_day, column], ignore_index=True)
                except:
                    print(f" filename: {filename}, name: {name},  :{column.columns[column.columns.duplicated(keep=False)]}")
                return df_day
    except:
        return df_day
    

def get_df_by_month(month):
    
    files = os.listdir(f'{ruta}/{month}')

    file_list = [f"{ruta}/{month}/{filename}" for filename in files if filename.endswith('.xlsx')]

    df_mp = pd.DataFrame()

    pool = Pool(processes=8)
    df_list = pool.map(read_csv_mp, file_list)

    for df in df_list:
        df_mp = pd.concat([df,df_mp])
    
    columnas = []
    for column in df_mp.columns:
        if(df_mp[f'{column}'].isnull().sum()<(df_mp.shape[0]*0.30)):
            columnas.append(column)
    
    df_final = df_mp[columnas]
    
    return df_final
    
    
if __name__ == '__main__':
    
    start_time = time.time()
    ruta = '/home/dmateons/Documents/mateo/python/xlsx_csv_to_sql/data/ORIGINAL'
    files_list = os.listdir(ruta)
    
    done_files = os.listdir(ruta.replace('ORIGINAL', 'csv_2'))
    done_files = [done.replace('.csv', "") for done in done_files if done.endswith('.csv')]
    
    for month in files_list:
        if month not in done_files:
            start = time.time()
            print(f'Procesando {month}')
            df = get_df_by_month(month)
            df.to_csv(f"{ruta.replace('ORIGINAL', 'csv_2')}/{month}.csv", index=False)
            
            print(f"Termino {month}, tiempo: {time.time() - start}")
    
    print("Tiempo total: ", (time.time()- start_time))
    
    #sql_service.set_data(df)
