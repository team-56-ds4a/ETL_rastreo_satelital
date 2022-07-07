import pandas as pd
import numpy as np
import os
import time



def read_csv(filename):
    return pd.read_excel(os.path.join('/home/dmateons/Documents/mateo/python/Parallel_Concurrent/data/MAYO 2021', filename),sheet_name=None)


def main():
    start_time = time.time()

    files = os.listdir('/home/dmateons/Documents/mateo/python/Parallel_Concurrent/data/MAYO 2021')
    data = pd.DataFrame()
    for filename in files:
        if filename.endswith('.xlsx'):
            df = read_csv(filename)
            data= pd.concat(df, ignore_index=True)

    print(data.shape)
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()