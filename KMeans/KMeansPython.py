""" A rough quickly written driver program for scikit-learn's K-Means
    clusterings algorithm, created to read csv files and compute a percentage
    score for how large a part of the sample set was clustered correctly.
"""
import sys
import csv
import numpy as np
from time import time
from sklearn import datasets
from sklearn.cluster import KMeans
from csv import writer

createCentroidCSVFile=False

# sys.argv[1] = Number of cluster (n_clusters)
# sys.argv[2] = Number of initial guesses on the starting centroids (n_init)
# sys.argv[3] = csv dataset
# FX: -> python KMmeansPython 4 1000 generated-test-samples-100-20-2.csv
# This will generate a csv file with centroid suggestions called "Centroids_" + str(n_clusters) + "-" + str(n_init) + sys.argv[3].

# Loads the a dataset and extract data and targets
if "-h" in sys.argv:
    print("usage: [samples.csv]")
    sys.exit(0)
elif len(sys.argv) == 1:
    iris = datasets.load_iris()
    X = iris.data
    Y = iris.target
    n_clusters = 3
    n_init=10
    max_iter=300
else:
    with open(sys.argv[3], 'rb') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',',)
        (X, Y) = ([], [])
        for line in csv_reader:
            line = map(lambda elem: float(elem), line)
            Y.append(int(line.pop()))
            X.append(np.array(line))
        X = np.array(X)
        Y = np.array(Y)
        n_clusters=int(sys.argv[1])
        n_init=int(sys.argv[2])		
        max_iter=0
        createCentroidCSVFile=True
        filename="centroids_" + str(n_clusters) + "-" + str(n_init) + "_" + sys.argv[3]

 
		
		
		
# Sets the initial centroids for use with KMeans, otherwise use KMeans++
# http://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html
centroids=np.array([[ 5.006, 3.418, 1.464, 0.244], [5.9016129, 2.7483871, 4.39354839, 1.43387097], [6.85, 3.07368421, 5.74210526, 2.07105263]])

# Configures the K-Means algorithm, documentation is at the link below
# http://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html
estimator = KMeans(n_clusters=n_clusters, init="k-means++", n_init=n_init, max_iter=max_iter,\
        tol=0.0001, precompute_distances=True, verbose=0, random_state=None,\
        copy_x=True, n_jobs=1)

# Runs the algorithm on the Iris dataset and compute a set of evaluation scores
start = time()
estimator.fit(X)
fitTime = time() - start

start = time()
score = estimator.score(X)
scoreTime = time() - start

start = time()
predicition = estimator.predict(X)
predictionTime = time() - start

correct_counter = 0.0
for pred, exp in zip(predicition, Y):
    if pred == exp:
        correct_counter += 1
correct_percentage = (correct_counter / Y.size) * 100

# Prints the small set of evaluation scores computed
print(50 * "-")
print("Fit-Time: " + str(fitTime))
print("Score-Time: " + str(scoreTime))
print("Prediction-Time: " + str(predictionTime))
print(50 * "-")
print("SCORE: " + str(score))
print("CORRECT (%): " + str(correct_percentage))
print("\nCENTROIDS: \n" + str(estimator.cluster_centers_))
print("\nPREDICTION: \n" + str(predicition))

if createCentroidCSVFile:
    with open(filename, 'ab') as csvfile:
      sample_writer = writer(csvfile, delimiter=',',)
      sample_list = list()
      for centroid in estimator.cluster_centers_:
          sample_writer.writerow(centroid)