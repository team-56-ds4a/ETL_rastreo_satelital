import pandas as pd
import sqlalchemy
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import time

import os
from dotenv import load_dotenv
load_dotenv()


def to_sql():
    
    HOST = os.getenv('HOST')
    USER = os.getenv('USERDB')
    PASS = os.getenv('PASSDB_LOCAL')
    DB = os.getenv('DATABASE')
    constring = f"mysql://{USER}:{PASS}@{HOST}/{DB}"
    dbEngine = sqlalchemy.create_engine(constring, connect_args={'connect_timeout': 10}, echo=False) 
    session = scoped_session(sessionmaker(bind=dbEngine))
    Base = declarative_base()
    Base.query = session.query_property()
    try:
        with dbEngine.connect() as con:
            con.execute("SELECT 1")
        print('engine is valid')
    except Exception as e:
        print(f'Engine invalid: {str(e)}')
        
    return dbEngine
    
if __name__ == '__main__':
    start = time.time()
    
    ruta = '/home/dmateons/Documents/mateo/python/Parallel_Concurrent/data/CSV'
    files_list = os.listdir(ruta)
    file_list = [f"{ruta}/{filename}" for filename in files_list if filename.endswith('.csv')]
    
    for file in file_list:
        dbengine = to_sql()
        df = pd.read_csv(file, engine='pyarrow')
        df = df.drop(['VELOCIDAD', 'ID'], axis=1)
        
        with dbengine.connect().execution_options(autocommit=True) as conn:
            df.to_sql('rastreo_satelital_v2', con=conn, if_exists='append', index= False)
                
    print(f"Tiempo final: {time.time() - start}")