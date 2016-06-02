import scipy
import pandas as pd
import numpy as np
from scipy.spatial.distance import *
import heapq
import sys
import time
import csv


def anomaly_check(read_fn, window, k):
    '''Checks for anomalous energy usage using a k nearest neighbors search. For
    every building, check all other buildings' usage during the previous time window
    (eg. 10 readings) against all other windows of the same length in the past.
    Finding the knn for the usage patterns, compare the usage of the building of
    interest against the same building's usage during the knn windows.'''

    window, k = int(window), int(k)
    chunksize = 1000
    dfc = pd.read_csv(read_fn, chunksize = chunksize, index_col = 0)
    df = pd.concat([chunk for chunk in dfc])
    df.fillna(0, inplace = True)
    meter_anom = []
    for column, values in df.iteritems():
        nearest_neighbors, baseY = run_window(df, column, window, k)
        knnl = []
        while len(nearest_neighbors) > 0:
            dist, y  = heapq.heappop(nearest_neighbors)
            knnl.append(y)
        averageY = np.mean(knnl, axis = 0)
        Ydist = euclidean(averageY, baseY)
        meter_anom.append((column, Ydist))
    with open("localresults.csv", "w") as f:
        csvf = csv.writer(f)
        for i in meter_anom:
            csvf.writerow(i)

def run_window(df, column, window, k):
    ''' Run the window over the entire data set to check for the knn, defined as
    the Euclidean distance of the flattened matrices of energy usage readings.'''

    knn = []
    heapq.heapify(knn)
    Y, X = df[column], df.ix[:, df.columns != column]
    baseX = X[:window].as_matrix().flatten()
    baseY = Y[:window].as_matrix()
    p = 0
    for i in range(window + 1, len(df)):
        if i + window > len(df) or i in range(p, window + p):
            continue
        comparison_matrix = X[i: i + window].as_matrix().flatten()
        dist = euclidean(baseX, comparison_matrix)
        if len(knn) < k:
            heapq.heappush(knn, (-dist, Y[i:i + window].as_matrix()))
            p = i
            continue
        min_dist, min_window = knn[0]
        if -dist > min_dist:
            heapq.heapreplace(knn, (-dist, Y[i: i + window].as_matrix()))
            p = i
    return knn, baseY

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python actobast.py <data file name> <windows> <k>")
    else:
        anomaly_check(sys.argv[1], sys.argv[2], sys.argv[3])
