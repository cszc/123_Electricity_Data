import pandas as pd
import numpy as np
import json, time
from argparse import ArgumentParser
from tarzan import *

def main():
    '''
    Takes a csvfile as an argument. Writes out csv to file.
    Example cl input:
    python tarzan-pipeline.py test.csv -a=16 -w=8 -f=4 -t=2
    '''
    parser = ArgumentParser()
    parser.add_argument('csv')
    parser.add_argument('-a', '--alpha_size', type=int, default=15, help='size of the alphabet with which to discretize')
    parser.add_argument('-w', '--window_length', type=int, default=4, help='Each step is half hour, so window length of 4 is 2 hours.')
    parser.add_argument('-f', '--feature_length', type=int, default=4, help='Size of rolling window to use when discretizing')
    parser.add_argument('-t', '--threshold', type=float, default=2, help='Threshold above which is considered surprising')
    parser.add_argument('--discretization', action="store_true", default=False, help='Only perform discretization and return strings')
    args = parser.parse_args()
    args_dict = vars(args)
    run_tarzan(**args_dict)


def get_discretization(df, alpha_size, feature_window):
    '''
    '''
    discretizations = {}
    for meter in df.columns[1:]:
        print(meter)
        try:
            ts = df[meter]
            building_dict = {
                'alpha_size':alpha_size,
                'feature_length':feature_window,
                }
            discretization = discretize(ts, alpha_size, feature_window)
            building_dict['discretization'] = discretize(ts, alpha_size, feature_window)
            discretizations[meter] = building_dict
            print(discretization + "\n")
        except:
            print('Error: Not enough variation in time series')
            continue

    with open('discretizations.json', 'w') as fp:
        json.dump(discretizations, fp, indent=4)


def run_tarzan(csv, alpha_size, window_length, feature_length, discretization=False, threshold=2):
    '''
    '''
    start = time.time()
    print("Starting")
    df = pd.read_csv(csv)
    meters = df.columns
    tarzan_dict = {}
    building_dict = {'alpha_size':alpha_size, 'window_length':window_length, 'feature_length':feature_length}

    if discretization:
        get_discretization(df, alpha_size, feature_length)

    else:
        for meter in df.columns[1:]:
            #convert each column to a time series
            ts = df.loc[:, ['datetime', meter]]
            print(meter)
            try:
                surprises, surprising_windows, scores, discretized = tarzan(
                    ts, alpha_size, window_length, feature_length,  meter, threshold)
                building_dict.update({
                    'scores': list(scores),
                    'surprising_windows':surprising_windows,
                    'surprises': list(surprises),
                    'startdate': ts['datetime'][0],
                    'discretized': discretized
                    })
                tarzan_dict['meter'] = building_dict
                print('surprising scores and indices:' + str(surprising_windows) + '\n')

            except:
                #If there is not enough variation for alphabet size,
                #the discretization will fail, e.g. the meter is off
                print('Error: Not enough variation in time series')
                continue

        with open('tarzan.json', 'w') as fp:
            json.dump(tarzan_dict, fp, indent=4)

    print("Finished {0} meters in {1}".format(
        len(meters), time.strftime(
            '%H:%M:%S', time.gmtime(start - time.time()))))


if __name__ == '__main__':
    main()
