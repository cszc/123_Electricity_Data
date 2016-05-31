from mrjob.job import MRJob
from mrjob.step import MRStep
import psycopg2 as pg2
import heapq
import pandas as pd
from pandas.io.sql import read_frame

class AnomalyDetection(MRJob):
    def connection_init(self):
        try:
            self.conn = pg2.connect("dbname = energy_data user = energy \
                    password = cs123")
            # print("Connection made")
        except Exception as e:
            print(e)

    def knn_init(self):
        self.k = 5
        self.knn = {}

    def heap_init(self):
        heap = []
        heapq.heapify(heap)
        return heap

    def mapper_init(self):
        self.connection_init()

    def mapper(self, _, line):
        window = 10

        lb, ub = line.split()[0], line.split()[1]
        query = "SELECT * FROM meters \
                WHERE id > {} and id <= {};".format(lb, ub)

        df = pd.read_sql_query(query, self.conn, index_col = "id")
        df.drop("datetime", inplace = True)

        self.knn_init()

        for meter in df.columns:
            nearest_neighbors = self.heap_init()
            Y, X = df[meter], df.ix[:, df.columns != meter]
            baseX = X[:window].as_matrix().flatten()
            baseY = Y[:window].as_matrix()
            p = 0
            for i in range(window + 1, len(df)):
                if i + window > len(df) or i in range(p, window + p):
                    continue
                comparison_matrix = X[i: i + window].as_matrix().flatten()
                dist = euclidean(baseX, comparison_matrix)
                if len(nearest_neighbors) < self.k:
                    Ydist = euclidean(Y[i:i + window].as_matrix(), baseY)
                    heapq.heappush(nearest_neighbors, (-dist, Ydist))
                    p = i
                    continue
                min_dist, minY = nearest_neighbors[0]
                if -dist > min_dist:
                    Ydist = euclidean(Y[i:i + window].as_matrix(), baseY)
                    heapq.heapreplace(nearest_neighbors, (-dist, Ydist))
                    p = i
            self.knn[meter] = nearest_neighbors

        for key, value in knn.items():
            while len(value) > 0:
                yield key, heapq.heappop()

    def mapper_final(self):
        self.conn.commit()
        if self.conn.closed:
            pass
        else:
            self.conn.close()

    def reducer_init(self):
        self.final_knn = {}

    def reducer(self, key, value):

        key_heap = self.heap_init()
        dist = value[0]
        if len(key_heap) < self.k:
            heapq.heappush(key_heap, value)
        min_dist, min_Y = key_heap[0]
        if dist > min_dist:
            heapq.heapreplace(key_heap, value)
        self.final_knn[key] = key_heap

    def reducer_final(self):
        for key, values in self.final_knn.items():
            dists = [x[1] for x in values]
            yield key, sum(dists) / len(dists)

if __name__ == '__main__':
    AnomalyDetection.run()
