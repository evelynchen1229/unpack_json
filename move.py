import json
import os
import re
import pandas as pd
from sqlalchemy import create_engine
import ast

def get_type(item, item_type):
    print_type = f"<class '{item_type}'>"
    return str(type(item)) == print_type

def get_dict_value(dictionary_value):
    return list(dictionary_value.values())[0]

def load_json_to_dataframe(file_name):
    with open(f'{file_name}','r+') as f:
        data = json.load(f)
    
    data_id = data['id']['theId']
    
    df = pd.json_normalize(data, 'actionList','id')
    number_of_records = len(df['musicActionJSON'])
    list_music_action = []
    for n in range(0,number_of_records):
        json_music = json.loads(df['musicActionJSON'][n])
        last_value = list(json_music.values())[-1]
        if get_type(last_value,'dict'):
            last_item = json_music.popitem()[-1]
            json_music = {**json_music, **last_item}
        
        list_music_action.append(json_music)
    
    df_music_action = pd.DataFrame(list_music_action)
    df_music_action['id']=data_id
    col =['id'] + list(df_music_action.columns[:-1])
    return df_music_action[col]

final_df = pd.DataFrame()
path_to_json = 'movedata/'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.JSON')]

for json_file in json_files:
    df = load_json_to_dataframe(path_to_json + json_file)
    final_df = pd.concat([final_df, df])

engine = create_engine('postgresql://evelyn:Indie$2912@localhost:5432/template')
final_df.to_sql('move_data',engine,index=False)
