import json
import pandas as pd

with open('box_122.json','r+') as f:
    content = file.read()
    file.seek(0)
    content.replace('{"theId":','')
    content.replace('},',',')
    data = json.load(f)

data = 
print(type(data))
df = pd.DataFrame.from_dict(data, orient='index')
df = df.transpose()
df = df.explode('sequenceId')

print(df['sequenceId'])

