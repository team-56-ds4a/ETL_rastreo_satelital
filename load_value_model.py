from multiprocessing import Pool
import numpy as np
import pandas as pd 
import joblib as jb
import osmnx as ox

    
def length(list1):
    prueba = []
    for i in range(len(list1)-1):
        try:
            prueba.append(G.edges[list1[i],list1[i+1],0]['length'])
        except:
            prueba.append(G.edges[list1[i],list1[i+1],1]['length'])
    return prueba

def df_tomodel(horas):
    df['LENGTH'] = df_data.LENGTH
    df["DAY"] = 1
    df["HOUR"] = horas
    return df   

if __name__ == '__main__':
    
    place = "Bogota, Colombia"
    G = ox.graph_from_place(place, network_type="drive")
    
    list1 = [331586541, 331585832, 331585384, 2099187366, 2099187385, 2099193734, 262178034, 1851469499, 325089228, 325089391, 325088888, 325088798, 611325810, 325089306, 325088596, 311255591, 311255598, 310979074, 310982449, 310981508, 310981605, 1043162937, 331592446, 427497436, 1043162951, 427497427, 427497426, 260480672, 260480645, 321551342, 610285638, 321551341, 610285646, 321551080, 321551051, 321551036, 321551038, 321551084, 815205006, 325093318, 261993797, 9215097609, 9215097615, 9215097614, 6498005320, 6498005232, 317215746, 6637941048, 6637941037, 1038706527, 261994878, 5228614837, 6637941056, 6637941057, 5228614836, 317213285, 317213198, 317206837, 317206912, 1038720464, 317204898, 317206226, 317204899, 317203063, 316162256, 317200637, 317200966, 316162257, 260697788, 5421597190, 2422242492, 260697728, 316162250, 317204767, 317204769]
    model = jb.load('model.pkl')
    
    df_data = pd.DataFrame(length(list1))
    df_data.columns = ['LENGTH']
    df = pd.DataFrame()
    
    horas = np.arange(1, 4, 1)
    pool = Pool(processes=8)
    df_list = pool.map(df_tomodel, horas)
    df_mp = pd.DataFrame()
    for df in df_list:
        df_mp = pd.concat([df,df_mp])
    print(df_mp)
    
    vel = model.predict(df_mp)
    
    print(vel)
    
    