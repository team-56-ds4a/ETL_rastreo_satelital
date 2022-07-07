import ssl
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
load_dotenv()
import os
import pandas as pd
import urllib
from sqlalchemy import event

ssl._create_default_https_context = ssl._create_unverified_context
HOST = os.getenv('HOST')
USER = os.getenv('USERDB')
PASS = os.getenv('PASSDB_LOCAL')
DB = os.getenv('DATABASE')

def check_done(dones_file, data_name):
    """check if a file has been already processed
    if the file was processed, returns true to continue with the next file
    Args:
        dones_file (str): path to the file containing the extracted xlsx
        data_name (str): name of the file to be checked
    Returns:
        bool: if true avoids the file, else continue processing the xlsx to csv
    """
    with open(dones_file, "r+") as file:
        lines = file.readlines()
        lines = [line.strip() for line in lines]
        
        if data_name in lines:
            print("Already done")
            return True
        
        file.write(data_name+"\n")
        
        return False

def set_data(df):
    
    connection_string = (
    f'DRIVER=MySQL ODBC 8.0 Driver;'
    f'SERVER=35.239.212.126;'
    f'DATABASE={DB};'
    f'UID={USER};'
    f'PWD={PASS};'
    )
    params = urllib.parse.quote_plus(connection_string)
    engine = create_engine("mysql+pyodbc:///?odbc_connect={}".format(params))
    engine.connect()
    
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
    ):
        if executemany:
            cursor.fast_executemany = True

    with engine.connect().execution_options(autocommit=True) as conn:
        df.to_sql('rastreo_satelital_v2', con=conn, if_exists='append', index= False, method='multi')
        

if __name__ == '__main__':
    
    ruta = '/home/dmateons/Documents/mateo/python/Parallel_Concurrent/data/CSV'
    files_list = os.listdir(ruta)
    file_list = [f"{ruta}/{filename}" for filename in files_list if filename.endswith('.csv')]
    
    for file in file_list:
        df = pd.read_csv(file, engine='pyarrow')
        df = df.drop(['VELOCIDAD', 'ID'], axis=1)
        set_data(df)