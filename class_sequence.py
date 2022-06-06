import json
import re
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy import orm as sa_orm
from sqlalchemy_redshift.dialect import TIMESTAMPTZ, TIMETZ


def get_type(item, item_type):
    print_type = f"<class '{item_type}'>"
    return str(type(item)) == print_type

def get_dict_value(dictionary_value):
    return list(dictionary_value.values())[0]

new_key = []
new_value = []

with open('box_122_copy.json','r+') as f:
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
df = df.explode('sequenceId')
#print(df['sequenceId'])

#engine = create_engine('postgresql://username:pw@host:port/database')
#df.to_sql('class_sequence',engine,index=False)
#df.to_csv('class_sequence',index=False)
#url = URL.create(
#        drivername = 'redshift+redshift_connector',
#        host = 'host', 
#        port = 5439,
#        database = 'dev',
#        username = 'username',
#        password = 'password'
#        )
#engine = sa.create_engine(url)
engine = create_engine('redshift+psycopg2://username:password@host:port/database')
df.to_sql('class_sequence',engine,schema = 'user_echen',if_exists='replace',index=False)
