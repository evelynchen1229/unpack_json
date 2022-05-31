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

new_key = ['id']
new_value = []

with open('movedata/09d9189f-6c25-4dee-9002-c8d74400f569.json','r+') as f:
    data = json.load(f)

for value in data.values():
    if get_type(value,'dict'):
       new_value.append(get_dict_value(value))
    elif get_type(value,'list'):
        for v in value:
            move_value_str = v['musicActionJSON']
            move_value_dict = ast.literal_eval(move_value_str)
            for (mk,mv) in zip(move_value_dict.keys(), move_value_dict.values()):
                if get_type(mv,'dict'):
                    for k in mv.keys():
                        new_key.append(k)
                    for v in mv.values():
                        new_value.append(v)
                else:
                    new_key.append(mk)
                    new_value.append(mv)
    else:
        pass

print(new_value[1:])
print(new_key[1:])
new_dict = dict(zip(new_key[1:],new_value[1:]))
print(new_dict)
df = pd.DataFrame.from_dict(new_dict, orient='index')
df = df.transpose()
print(df)
#df = df.explode('sequenceId')
##print(df['sequenceId'])
#
#path_to_json = 'sequence/'
#json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
##engine = create_engine('postgresql://evelyn:Indie$2912@localhost:5432/template')
##df.to_sql('class_sequence',engine,index=False)
