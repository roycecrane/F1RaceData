import pandas as pd
import json


def load_data_local(file_name, file_type):
    file_name = file_name + '.' + file_type
    try:
        if file_type == 'xlsx':
            return pd.read_excel(file_name)
        if file_type == 'csv':
            return pd.read_csv(file_name)
    except:
        print(f'error: could not load file: {file_name}\n')
        return pd.DataFrame()


def save_data(df, file_name, file_type):
    df = df.reset_index()
    if 'index' in df:
        df = df.drop(['index'], axis=1)
    if file_type == 'xlsx':
        df.to_excel(f'{file_name}.{file_type}',index=False)
    if file_type == 'csv':
        df.to_csv(f'{file_name}.{file_type}',index=False)
