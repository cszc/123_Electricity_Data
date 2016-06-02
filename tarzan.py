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
import string
from suffix_array import *
from operator import mul
from functools import reduce

alphabet = list(string.ascii_letters)

def discretize(ts, alpha_size, feature_window):
    '''
    ts: reference time series (numpy vector)
    x_ts:
    alpha_size: alphabet size (how many tokens/buckets)
    feature_window: time window
    '''
    assert alpha_size <= len(alphabet), "alphabet size must be less than {0}".format(len(alphabet))

    windows = get_rolling_window(ts, feature_window)
    slopes = np.empty(ts.shape[0] - feature_window + 1)
    slopes.fill(np.nan)
    y = np.array(range(0, feature_window))

    #Slide the feature window over the time series, and at each step calculate
    #the slope of the window
    for i, window in enumerate(windows):

        slopes[i] = get_slope(window, y)

    #Take all of the computed scores and replace them with labels created from
    #quantiled bins
    tokens = pd.qcut(slopes, alpha_size, labels=alphabet[:alpha_size])

    #casting pandas series of categories to a single string
    return "".join(list(tokens))


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


def get_longest_substring(suffix, str_r):
    '''
    '''
    #
    for l in reversed(range(len(suffix))):
        i = 0
        found_longest = True
        #
        while (i + l) <= len(suffix):
            substring = suffix[i:i+l]
            #
            if substring not in str_r:
                found_longest = False
                break
            i += 1
        #if while loop completed, return length of longest substring
        if found_longest:
            return l


def get_expected_value(w, str_r, scale_factor, x_factor):
    '''
    '''
    count = str_r.count(w)
    expected_value = None

    if count:
        expected_value = count

    else:
        l = get_longest_substring(w, str_r)
        i = 0
        numerator = []
        denominator = []

        if l > 1:
            while (i + l) <= len(w):
                substring = w[i:i+l]
                subsuffix = w[i+1:i+l-1]
                numerator.append(str_r.count(substring))
                denominator.append(str_r.count(subsuffix))
                i += 1
            numerator = reduce(mul, numerator)
            denominator = reduce(mul, denominator)

        else:
            scale_factor = x_factor
            for letter in suffix:
                numerator.append(str_r.count(letter))
            denominator = reduce(mul, len(r_ts)**(len(numerator)))
            numerator = reduce(mul, numerator)
        expected_value = (numerator/denominator)

    return scale_factor*expected_value


def score_windows(str_r, str_x, window, threshold=2):
    r = len(str_r)
    x = len(str_x)
    scale_factor = ((x - window + 1) / (r - window + 1))
    scores = []
    surprises = []
    for i in range(x - window + 1):
        #setting window to size
        w = str_x[i:i + window - 1]
        frequency = str_x.count(w)
        expected_value = get_expected_value(w, str_r, scale_factor, (x - window + 1))
        score = frequency - expected_value
        scores.append(score)
        if score > threshold:
            surprises.append((score, i))
    return scores, surprises


def tarzan(series, alpha_size, window_length, feature_length, col_name, threshold=2):
    one_week = 2*24*7 #2 readings per hour each day for seven days
    ts = series[col_name]
    start_of_x = len(ts) - one_week
    r_ts, x_ts = ts[:start_of_x], ts[start_of_x:]
    r = discretize(r_ts, alpha_size, feature_length)
    x = discretize(x_ts, alpha_size, feature_length)
    print(x)
    scores, surprises = score_windows(r, x, window_length, threshold)
    surprising_windows = []
    for surprise in surprises:
        score, index = surprise
        surprising_windows.append((x_ts[feature_length*index : feature_length*index + feature_length], score))

    return (surprising_windows, scores, col_name)



# df = pd.read_csv('MetersAsColumns.csv')
# rts = df['A06 Crerar Library (B1)']
# a = discretize(rts, 50, 4)
# a = [str(x) for x in a.tolist()]
# print(a)

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
