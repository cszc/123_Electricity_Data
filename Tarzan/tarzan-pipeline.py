import pandas as pd
import numpy as np
import json
from tarzan import *

ALPHA_SIZE
WINDOW_LENGTH
FEATURE_LENGTH

print("it's starting")
df = pd.read_csv('MetersAsColumns.csv')
meters = df.columns

print('starting')

surprises = []
results = []

tarzan_dict = {'meter':None, 'score': 'window'None}

for meter in df.columns[1:]:
    #convert each column to a time series
    ts = df.loc[:, ['datetime', meter]]
    print(meter)
    try:
        surprising_windows, scores, col_name = tarzan(ts, 16, 8, 4, meter, threshold=1.5)
        surprises.append((surprising_windows, col_name))
        results.append((scores, col_name))
        print(surprising_windows)
        print(scores)
    except:
        print('IT EXCEPTED')
        continue

print(results, surprises)
