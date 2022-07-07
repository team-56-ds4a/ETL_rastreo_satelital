import numpy as np

def duplicated_columns(df, names=True):
    duplicates = []
    for col in range(df.shape[1]):
        for comp in range(col + 1, df.shape[1]):
            if df.iloc[:, col].equals(df.iloc[:, comp]):
                duplicates.append(comp)
    
    duplicates = np.unique(duplicates).tolist()
    
    if names:
        return df.columns[duplicates]
    else:
        return duplicates
    
def remove_duplicated_columns(df):
    duplicates = duplicated_columns(df)
    
    if len(duplicates) == 0:
        return df
    else:
        return df.drop(duplicates, axis=1)