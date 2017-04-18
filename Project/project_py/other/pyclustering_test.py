import os
import sys

# an example of clustering by BIRCH algorithm.
from pyclustering.cluster import cluster_visualizer
from pyclustering.cluster.birch import birch;
from pyclustering.samples.definitions import FCPS_SAMPLES

from pyclustering.utils import read_sample

# Path for spark source folder
os.environ['SPARK_HOME']="/Users/Nikita/hadoop/spark-2.1.0-bin-hadoop2.7/"

# Append pyspark  to Python Path
sys.path.append("/Users/Nikita/hadoop/spark-2.1.0-bin-hadoop2.7/python/")

# try:
#     from pyspark import SparkContext
#     from pyspark import SparkConf
#     print ("Successfully imported Spark Modules")
# except ImportError as ex:
#     print ("Can not import Spark Modules", ex)
#     sys.exit(1)

# load data from the FCPS set that is provided by the library.
sample = read_sample(FCPS_SAMPLES.SAMPLE_LSUN)

# create BIRCH algorithm for allocation three objects.
birch_instance = birch(sample, 3)

# start processing - cluster analysis of the input data.
birch_instance.process()

# allocate clusters.
clusters = birch_instance.get_clusters()

# visualize obtained clusters.
visualizer = cluster_visualizer()
visualizer.append_clusters(clusters, sample)
visualizer.show()
