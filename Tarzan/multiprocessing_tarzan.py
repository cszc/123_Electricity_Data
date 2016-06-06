import pandas as pd
import numpy as np
from tarzan import *
from argparse import ArgumentParser
from multiprocessing import Process, JoinableQueue, freeze_support

q = JoinableQueue()
processes = []
surprises = []
results = []


def main():
    parser = ArgumentParser()
    parser.add_argument('csv')
    parser.add_argument('num_processes')
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    meters = df.columns
    num_processes = int(args.num_processes)

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

    for i in range(num_processes):
        p = Process(target=worker)
        p.start()
        processes.append(p)

    # block until all tasks are done
    print('here')
    p.join()
    print('now here')

    # stop workers
    for i in range(num_processes):
        q.put(None)
    for p in processes:
        p.join()

    q.join()
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
            print(item['col_name'] + ":done")
            q.task_done()


if __name__ == '__main__':
    freeze_support()
    main()
