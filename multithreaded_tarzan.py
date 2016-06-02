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
    print('here')
    q.join()
    print('now here')

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
            surprising_windows, scores, col_name = tarzan(**item)
            surprises.append((surprising_windows, col_name))
            results.append((scores, col_name))
        except:
            print(item['col_name']+ ": Excepted")
            continue
        finally:
            print(item + ":done")
            q.task_done()


if __name__ == '__main__':
    main()
