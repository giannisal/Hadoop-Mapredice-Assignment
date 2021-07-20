import sklearn.datasets as dt
from numpy import savetxt
import csv


# Testing with 12 datapoints in total
# n_samples = 12
n_samples = 1200000
centers = [(5,1),(-1,-3),(-4,6)]
x, y = dt.make_blobs(n_samples=n_samples, n_features=2,
        cluster_std=0.8, centers=centers, shuffle=True)
# Testing by printing the output, both the points and their respective clusters
# print(x, y)
#exporting our datapoints to csv format
x = x.astype('float64')
savetxt('hadoopdata.txt', x, delimiter=',', fmt='%f')

# exporting our centroids to csv format
# Using a different way of saving to CSV,
#for the sake of exploring alternatives
savetxt('hadoopcentroids.txt', centers, delimiter=',', fmt='%f')
