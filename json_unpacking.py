import json
import re
import pandas as pd

def get_type(item, item_type):
    print_type = f"<class '{item_type}'>"
    return str(type(item)) == print_type

def get_dict_value(dictionary_value):
    return list(dictionary_value.values())[0]


with open('box_122_copy.json','r+') as f:
    data = json.load(f)
    for d in data:
        new_key.append(d)

new_key = []
new_value = []

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
df = df.explode('sequenceId')
print(df['sequenceId'])

