import pandas as pd
import numpy as np
import sys


def go(read_fn, write_fn):
    #take the Date and Time strings and convert into one datetime column
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
    chunksize = 10000
    dfc = pd.read_csv(read_fn, chunksize = chunksize, header=3, parse_dates={'datetime': ['Date', 'Start Time']}, \
                      date_parser = dateparse, index_col=0)

    df = pd.concat([chunk for chunk in dfc])

    #### Add terms/breaks to dates
    dates = ['2014-04-10', '2014-06-04', '2014-06-14',
    '2014-06-23', '2014-08-31', '2014-09-28', '2014-12-03',
    '2014-12-13', '2015-01-04', '2015-03-11', '2015-03-21',
    '2015-03-29', '2015-06-03', '2015-06-13', '2015-06-21',
    '2015-08-29', '2015-09-27', '2015-12-03', '2015-12-12',
    '2016-01-03', '2016-03-09', '2016-03-19', '2016-03-27',
    '2016-05-10']

    labels = ['Spring 14', 'Reading/Exam Spring 14', 'Break Spring 14',
    'Summer 14', 'Break Summer 14', 'Fall 14', 'Reading/Exam Fall 14',
    'Break Fall 14', 'Winter 15', 'Reading/Exam Winter 15', 'Break Winter 15',
    'Spring 15', 'Reading/Exam Spring 15', 'Break Spring 15', 'Summer 15',
    'Break Summer 15', 'Fall 15', 'Reading/Exam Fall 15', 'Break Fall 15',
    'Winter 16', 'Reading/Exam Winter 16', 'Break Winter 16', 'Spring 16']

    i = 0
    uniquedays = []
    terms = []
    for date in df_final.index.unique():
        if (str(date) > dates[i]) and (str(date) <= dates[i + 1]):
            terms.append(labels[i])
            uniquedays.append(date)
        else:
            i += 1
            terms.append(labels[i])
            uniquedays.append(date)

    termlabels = dict(zip(uniquedays, terms))
    f = lambda x: termlabels[x]
    df_final['Term'] = df_final.index.map(f)

    #### Write to csv
    df_final.to_csv(write_fn)

if __name__ == '__main__':
    if len(sys.argv) == 3:
        go(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python hourlybybuilding.py <full meter file name> <write filename>")
