import pandas as pd

fn = 'datasets/dynamodb_20180823.json'
df = pd.read_json(fn)
df.info()
df.head()


len_cards = len(response)
len_ok = len(df[df['status'] == 'OK'])
print("cards=%d\tOK=%d\trate=%d%%" % (len_cards, len_ok, len_ok / len_cards * 100))

df.iloc[0]['data']


import seaborn as sns
import matplotlib.pyplot as plt
%matplotlib inline

with_trips = df[df['status'] == 'OK']['number']
len(with_trips)
plt.figure(figsize=(12, 8))
plt.hist(with_trips, bins=30)
