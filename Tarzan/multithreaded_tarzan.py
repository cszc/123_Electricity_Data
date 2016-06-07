import pandas as pd
import numpy as np
from tarzan import *
import queue
from argparse import ArgumentParser
import threading

q = queue.Queue()
threads = []
surprises = []
results = []


def main():
    parser = ArgumentParser()
    parser.add_argument('csv')
    parser.add_argument('num_worker_threads')
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    meters = df.columns
    num_worker_threads = int(args.num_worker_threads)

    for meter in df.columns[1:]:
        ts = df.loc[:, ['datetime', meter]]
        var_dict = {
            "series": ts,
            'alpha_size':16,
            'window_length':8,
            'feature_length':4,
            'col_name':meter,
            'threshold':1.5}
        q.put(var_dict)

    for i in range(num_worker_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    # block until all tasks are done
    q.join()

    # stop workers
    for i in range(num_worker_threads):
        q.put(None)
    for t in threads:
        t.join()

    print(results, surprises)

def worker():
    while True:
        item = q.get()
        if item is None:
            break
        try:
            print(item['col_name'])
            surprising_windows, surprises, scores, x = tarzan(**item)
            print(x)
            print(scores)
            print()
            surprises.append((surprising_windows, item['col_name']))
            results.append((scores, item['col_name']))
        except Exception as e:
            #Possible exceptions include numpy.polyfit failing on NaN or other
            #values. Additionally, if there isn't enough variation in the data,
            #Pandas is unable to create bins to create an alphabet. This usually
            #happens when the meter is unused and is all 0s.
            print(e)
            print(item['col_name']+ ": Excepted\n")
            continue
        finally:
            print(item['col_name'] + ":done\n")
            q.task_done()


if __name__ == '__main__':
    main()
