'''
Discretizing/Tokenizing the time series. O(N) time.

Based on: http://www.cs.ucr.edu/~eamonn/sigkdd_tarzan.pdf

With help from: https://blog.acolyer.org/2016/05/09/finding-surprising-patterns-in-a-time-series-database-in-linear-time-and-space/
'''
#Tarzan Anomaly Detection Algorithm
#Big Data Questions:
#How to map reduce this
#How to do it on different distributed databases
#Other ways of making it fater?
#Is there a way we can store or precalcuate the discretized versions in order to query faster,
#maybe given a pre determined set of windows
import numpy as np
import pandas as pd
import warnings
#warnings.simplefilter('ignore', np.RankWarning)


def discretize(ts, alpha_size, feature_window, degree):
    '''
    ts: reference time series (numpy vector)
    x_ts:
    alpha_size: alphabet size (how many tokens/buckets)
    feature_window: time window
    degree: degree of polynomial
    '''
    windows = get_rolling_window(ts, feature_window)
    slopes = np.empty(ts.shape)
    print(ts.shape)
    slopes.fill(np.nan)
    y = np.array(range(0, feature_window))

    #Slide the feature window over the time series, and at each step calculate
    #the slope of the window
    for i, window in enumerate(windows):

        slopes[i] = get_slope(window, y)

    #Take all of the computed scores and replace them with labels created from
    #quantiled bins
    tokens = pd.cut(slopes, alpha_size, labels=range(alpha_size))

    return tokens


def get_rolling_window(a, window):
    '''
    a = array
    window = window size
    http://www.rigtorp.se/2011/01/01/rolling-statistics-numpy.html
    '''
    shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
    strides = a.strides + (a.strides[-1],)
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

def get_slope(series, y, degree=1):
    '''
    series is an numpy array
    degree is an integer
    '''
    x = np.array(series)
    #if all elements in an array are the same, e.g. [0, 0]
    if np.unique(x).size == 1:
        slope = 0
    else:
        slope = np.polyfit(x, y, degree)[0]

    return slope

df = pd.read_csv('MetersAsColumns.csv')
rts = df['A06 Crerar Library (B1)']
a = discretize(rts, 50, 4, 1)
print(a)

"""
1. Choose and alphabet size and feature window length
slide the feature window over the time series

2. func examine the data and computer a single real number from it
(slope of best fitting line)

take all of the computed scores and sort them
based on sorted list, derive alphabet size buckets
equal number of scores per bucket
go back, and replace each score by the label of the bucket it falls into
"""

#
