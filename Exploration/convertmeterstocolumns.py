import pandas as pd
import numpy as np

def go():
    read_fn = "Full_Electric_Interval_042016.csv"
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
    chunksize = 10000
    dfc = pd.read_csv(read_fn, chunksize = chunksize, header=3, parse_dates={'datetime': ['Date', 'Start Time']}, \
                      date_parser = dateparse, index_col=0)

    df = pd.concat([chunk for chunk in dfc])
    print("Loaded dataframe...")
    df = df[~df.Meter.str.contains("BLANK")]

    df_final = pd.DataFrame(index = df.index.unique())

    for meter in df.Meter.unique():
        if meter == "datetime":
            continue
        print("Converting ", meter, " to column...")
        new_col = df[df.Meter == meter]
        try:
            df_final[new_col.Meter[1]] = new_col.Usage.fillna(0)
        except Exception as e:
            print(meter, e)

    df_final.to_csv("MetersAsColumns.csv")

if __name__ == '__main__':
    go()
