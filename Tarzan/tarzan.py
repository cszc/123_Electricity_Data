'''
Discretizing/Tokenizing the time series. O(N) time.

Based on: http://www.cs.ucr.edu/~eamonn/sigkdd_tarzan.pdf

Interpretation with help from: https://blog.acolyer.org/2016/05/09/finding-surprising-patterns-in-a-time-series-database-in-linear-time-and-space/
'''

import numpy as np
import pandas as pd
import warnings
import string
from suffix_array import *
from operator import mul
from functools import reduce

#Alphabet can contain max 52 characters: all lower and upper case ascii letters.
ALPHABET = list(string.ascii_letters)

def discretize(ts, alpha_size, feature_window):
    '''
    Takes a numpy time series and returns a string representing a discretized
    version of the time series.
    Input:
        ts: numpy array - reference time series (numpy vector)
        alpha_size: int - alphabet size (how many tokens/buckets)
        feature_window: int - time window used to discretize the time series
    Output:
        string, e.g. 'odobbbbicmknooppphhjpanhocnccccdddeeeaaapajjiaaapnnfccmb'
    '''
    assert alpha_size <= len(ALPHABET), "alphabet size must be less than {0}".format(len(ALPHABET))

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
    tokens = pd.qcut(slopes, alpha_size, labels=ALPHABET[:alpha_size])

    #casting pandas series of categories to a single string
    return "".join(list(tokens))


def get_rolling_window(a, window):
    '''
    Takes a time series and a window size and returns a matrix with each nested
    array representing a window in the time series.
    Taken from http://www.rigtorp.se/2011/01/01/rolling-statistics-numpy.html

    Input:
        a: numpy array - time series
        window: int r- window size

    Output:
        numpy array of arrays, e.g. array([[[0, 1, 2], [1, 2, 3], [2, 3, 4]],
                                        [[5, 6, 7], [6, 7, 8], [7, 8, 9]]])
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
    #if all elements in an array are the same, return 0 e.g. [0, 0]
    if np.unique(x).size == 1:
        slope = 0
    else:
        #get coefficient of highest degree; degree is always 1.
        slope = np.polyfit(x, y, degree)[0]
    return slope


def get_longest_substring(suffix, str_r):
    '''
    Takes a suffix and a reference string, and returns l, the length such that
    every possible substring of length l in the suffix is present in the reference
    string.
    Input:
        suffix: string
        str_r: string (reference string)
    Output:
        l: int, length of longest substring
    '''
    #l is maximum length of a substring
    for l in reversed(range(len(suffix))):
        i = 0
        found_longest = True
        #substring shouldn't go past end of suffix
        while (i + l) <= len(suffix):
            substring = suffix[i:i+l]
            #check if each substring of length l is present in reference string
            if substring not in str_r:
                found_longest = False
                break
            i += 1
        #if while loop completed, return length of longest substring
        if found_longest:
            return l


def get_expected_value(w, str_r, scale_factor, x_factor):
    '''
    Takes a word, reference string, scale factor, and
    If the full substring is found in the reference string, the expected value
    is simply the number of times (count) the substring appears in the refence string.

    Else if, if only substrings of length > 1 can be found in the reference string,
    the expected value is the multipication of the counts for each substring
    in the reference string, divided by the multiplication of the counts of
    sub-substrings, "subsuffixes".

    Else, if only strings of size <=1 can be found, the expected value is the
    probability of seeing each individual string in the reference string multiplied
    together. Please see the original paper for more details.

    Input:
        w: string - window to look for in reference string
        str_r: string - reference string
        scale_factor: float - factor to normalize score by size of strings
        x_factor: float - alternative factor if entire window w isn't found in r
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
