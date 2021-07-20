from mrjob.job import MRJob
import os
import numpy as np

class Kmeanshadoop(MRJob):

    def mapper(self, _, line):
        centroids = []
        centers = (os.environ['CENTROIDS'])
        #centers = '/home/giannis/Downloads/hadoopcentroids.txt'
        with open (centers) as c:
            for cent in c:
                cent = cent.strip()
                point = cent.split(',')
                centroids.append([ float(point[0]) , float(point[1])])
        #datafile = '/home/giannis/Downloads/hadoopdata.txt'
        #with open (datafile) as line:
        data = line.strip()
        point = data.split(',')
        try:
            data =[float(point[0]), float(point[1])]
        except Exception as exc:
            print('ok')
            #print(str(line) + 'data' + str(data))
            #print(bytes(line, 'utf-8'))
            #print(exc)
        min_dist = np.linalg.norm(np.array(data) - np.array(centroids[0]))
        cluster = 0
        for i in range(1,len(centroids)):
            dist = np.linalg.norm(np.array(data) - np.array(centroids[i]))
            if dist < min_dist:
                min_dist = dist
                cluster = i
            yield cluster, data



        #yield "chars", len(line)
        #yield "words", len(line.split())
        #yield "lines", 1
    def reducer(self, key, values):
        '''
        x_sum = 0
        y_sum = 0
        count = 0
        cluster_ind = None
        cluster = key
        x, y = values.split(',')
        try:
            x = float(x)
            y = float(y)
        except ValueError:
            continue
        if cluster_ind==cluster
            x_sum +=x
            y_sum += y
            count +=1
        else:
            try:
                print(str(x_sum/count + "," + str(y_sum/count)))
            except ZeroDivisionError:
                continue

        x_sum =x
        y_sum = y
        count = 1
        '''
        count = 0
        x_sum = 0.0
        y_sum = 0.0
        for points in values:
            count +=1
            x_sum += points[0]
            y_sum += points[1]

        yield(key, (x_sum/count,y_sum/count))

if __name__ == '__main__':
    Kmeanshadoop.run()
