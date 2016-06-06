from mrjob.job import MRJob
from mrjob.step import MRStep
import psycopg2 as pg2
import heapq
import pandas as pd
from scipy.spatial.distance import *

class AnomalyDetection(MRJob):
    '''Runs a k-nearest neighbors (knn) search of energy usage for University of Chicago
    buildings to try to detect anomalous usage. Looking at one building (say, Ryerson)
    at a time, it compares all other buildings' energy behavior during a specified
    window length to find the k-nearest neighbors (defined by Euclidean distance
    of the flatten matrices) of similar energy usage. It then compares the usages
    of that building (Ryerson) during those windows to the usage during the most
    recent window to determine if the usage is out of a reasonable boundary.'''

    # K and windowsize can be adjusted
    k = 5
    window = 10

    def knn_init(self):
        '''Initialize dictionary where keys will be building names and values will
        be heaps of the current k nearest neighbors.'''
        self.knn = {}

    def heap_init(self):
        '''Initializes a max heap using the heapq library.'''
        heap = []
        heapq.heapify(heap)
        return heap

    def mapper_init(self):
        '''Connect to Postgres database with energy usage data'''
        try:
            self.conn = pg2.connect("dbname = energy_data user = energy \
                    password = cs123")
        except Exception as e:
            print(e)
            print("Could not connect to database.")
            return

    def mapper(self, _, line):
        ''' Reads a line with query parameters and maps results to process in chunks.
        After querying for results, checks over each building to find the knn. Stores
        the knn so far in a heap with the distance of the comparison matrix and the
        distance of the building of interest.'''

        # Select the time of interest, most recent window, for comparison
        base = "SELECT * FROM meters \
                WHERE id <= {};".format(self.window - 1)
        base_time = pd.read_sql(base, self.conn, index_col = "id")
        base_time.drop("datetime", axis = 1, inplace = True)

        lb, ub = line.split()[0], line.split()[1]
        # For first chunk, do not compare the window against itself
        if int(lb) < self.window:
            lb = self.window

        query = "SELECT * FROM meters \
                WHERE id > {} and id <= {};".format(lb, ub)

        df = pd.read_sql(query, self.conn, index_col = "id")
        df.drop("datetime", axis = 1, inplace = True)

        self.knn_init()

        for meter in df.columns:
            nearest_neighbors = self.heap_init()
            Y, X = df[meter], df.ix[:, df.columns != meter]
            baseY, baseX = base_time[meter].as_matrix(), \
                        base_time.ix[:, base_time.columns != meter].as_matrix().flatten()
            p = 0
            for i in range(self.window + 1, len(df)):
                # Setting p prevents overlapping windows
                if i + self.window > len(df) or i in range(p, self.window + p):
                    continue
                comparison_matrix = X[i: i + self.window].as_matrix().flatten()
                dist = euclidean(baseX, comparison_matrix)
                # Fill heap until length of k
                if len(nearest_neighbors) < self.k:
                    Ydist = euclidean(Y[i:i + self.window].as_matrix(), baseY)
                    heapq.heappush(nearest_neighbors, (-dist, Ydist))
                    p = i
                    continue
                max_dist, maxY = nearest_neighbors[0]
                # Heapq implements a max heap where minimum values are replaced.
                # Distance is set to negative so that max distance will be popped
                if -dist > max_dist:
                    Ydist = euclidean(Y[i:i + self.window].as_matrix(), baseY)
                    heapq.heapreplace(nearest_neighbors, (-dist, Ydist))
                    p = i
            self.knn[meter] = nearest_neighbors

        for key, value in self.knn.items():
            while len(value) > 0:
                yield key, heapq.heappop(value)

    def mapper_final(self):
        '''Close connection to the database'''
        print("Mapper final")
        self.conn.commit()
        if self.conn.closed:
            pass
        else:
            self.conn.close()

    def reducer_init(self):
        '''Initialize dictionary to store the final knn, reducing the knn from each
        of the mappers to a single result.'''
        print("Reducer Init")
        self.final_knn = {}

    def reducer(self, key, value):
        '''Goes through the knn for each of the nodes and creates a dictionary with
        only the top k overall nearest neighbors.'''
        print("Reducer main")
        key_heap = self.heap_init()
        dists = list(value)
        for dist, Ydist in dists:
            if len(key_heap) < self.k:
                heapq.heappush(key_heap, (dist, Ydist))
            max_dist, maxY = key_heap[0]
            if dist > max_dist:
                heapq.heapreplace(key_heap, (dist, Ydist))
        self.final_knn[key] = key_heap

    def reducer_final(self):
        '''Yields the building name and average distance from k nearest neighbors'''
        print("Reducer Final")
        for key, values in self.final_knn.items():
            dists = [x[1] for x in values]
            yield key, sum(dists) / len(dists)

if __name__ == '__main__':
    AnomalyDetection.run()
