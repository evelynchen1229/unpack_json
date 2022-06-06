import json
import os
import re
import pandas as pd
from sqlalchemy import create_engine

def get_type(item, item_type):
    print_type = f"<class '{item_type}'>"
    return str(type(item)) == print_type

def get_dict_value(dictionary_value):
    return list(dictionary_value.values())[0]

def load_json_to_dataframe(file_name):
    new_key = []
    new_value = []
    
    with open(f'{file_name}','r+') as f:
        data = json.load(f)
        for d in data:
            new_key.append(d)
    
    
    for value in data.values():
        if get_type(value,'dict'):
           new_value.append(get_dict_value(value))
        elif get_type(value,'list'):
            list_value = [get_dict_value(v) for v in value]
            new_value.append(list_value)
        else:
            new_value.append(value)
    
    new_dict = dict(zip(new_key,new_value))
    df = pd.DataFrame.from_dict(new_dict, orient='index')
    df = df.transpose()
    return df

final_df = pd.DataFrame()
path_to_json = 'sequence/'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

for json_file in json_files:
    df = load_json_to_dataframe(path_to_json + json_file)
    final_df = pd.concat([final_df,df])

#engine = create_engine('postgresql://username:pw@host:port/database')
engine = create_engine('redshift+psycopg2://username:pw@host:port/database')
final_df.to_sql('sequence_move',engine,schema = 'user_echen',if_exists = 'replace',index=False)
