#!/usr/bin/env python

#import sys

#print (sys.version)

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
#
# import mpl_toolkits
# import enum
from pyclustering.utils import read_sample;
import pyclustering.cluster.birch
#
conf = SparkConf().setAppName("NikitaBaranov.Project:Cure Clustering").setMaster("local")
sc = SparkContext(conf=conf)

#
## load data from the FCPS set that is provided by the library.
#sample = read_sample(FCPS_SAMPLES.SAMPLE_LSUN);
sample = read_sample("/Users/Nikita/hadoop/project_py/data/SampleLsun.txt");

# an example of clustering by BIRCH algorithm.
from pyclustering.cluster.birch import birch;
from pyclustering.utils import read_sample;

# load data from the FCPS set that is provided by the library.
sample = read_sample(FCPS_SAMPLES.SAMPLE_LSUN);
# create BIRCH algorithm for allocation three objects.
birch_instance = birch(sample, 3);
# start processing - cluster analysis of the input data.
birch_instance.process();
# allocate clusters.
clusters = birch_instance.get_clusters();
# visualize obtained clusters.
visualizer = cluster_visualizer();
visualizer.append_clusters(clusters, sample);
visualizer.show()

#
#print sample
#
## create BIRCH algorithm for allocation three objects.
birch_instance = pyclustering.cluster.birch.birch(sample, 3);
#
## start processing - cluster analysis of the input data.
birch_instance.process();
#
## allocate clusters.
clusters = birch_instance.get_clusters();
#
## visualize obtained clusters.
visualizer = cluster_visualizer();
visualizer.append_clusters(clusters, sample);
visualizer.show();
#
#
#
#from pyclustering.utils import read_sample;
#from pyclustering.utils import timedcall;
#
#from pyclustering.samples.definitions import SIMPLE_SAMPLES;
#from pyclustering.samples.definitions import FCPS_SAMPLES;
#
#from pyclustering.cluster import cluster_visualizer;
#from pyclustering.cluster.cure import cure;
#
#
#def template_clustering(number_clusters, path, number_represent_points = 5, compression = 0.5, draw = True, ccore_flag = False):
#    sample = read_sample(path);
#    
#    cure_instance = cure(sample, number_clusters, number_represent_points, compression, ccore_flag);
#    (ticks, _) = timedcall(cure_instance.process);
#    
#    clusters = cure_instance.get_clusters();
#    representors = cure_instance.get_representors();
#    means = cure_instance.get_means();
#    
#    print("Sample: ", path, "\t\tExecution time: ", ticks, "\n");
#
#    if (draw is True):
#        visualizer = cluster_visualizer();
#        
#        if (ccore_flag is True):
#            visualizer.append_clusters(clusters, sample);
#            
#        else:
#            visualizer.append_clusters(clusters, None);
#        
#        for cluster_index in range(len(clusters)):
#            visualizer.append_cluster_attribute(0, cluster_index, representors[cluster_index], '*', 10);
#            visualizer.append_cluster_attribute(0, cluster_index, [ means[cluster_index] ], 'o');
#        
#        visualizer.show();
#
#
#def cluster_sample1():
#    template_clustering(2, SIMPLE_SAMPLES.SAMPLE_SIMPLE1);
#
#def cluster_sample2():
#    template_clustering(3, SIMPLE_SAMPLES.SAMPLE_SIMPLE2);
#
#def cluster_sample3():
#    template_clustering(4, SIMPLE_SAMPLES.SAMPLE_SIMPLE3);
#
#def cluster_sample4():
#    template_clustering(5, SIMPLE_SAMPLES.SAMPLE_SIMPLE4); 
#
#def cluster_sample5():
#    template_clustering(4, SIMPLE_SAMPLES.SAMPLE_SIMPLE5);
#
#def cluster_sample6():
#    template_clustering(2, SIMPLE_SAMPLES.SAMPLE_SIMPLE6);
#
#def cluster_elongate():
#    template_clustering(2, SIMPLE_SAMPLES.SAMPLE_ELONGATE);
#
#def cluster_lsun():
#    template_clustering(3, FCPS_SAMPLES.SAMPLE_LSUN, 5, 0.3);
#    
#def cluster_target():
#    template_clustering(6, FCPS_SAMPLES.SAMPLE_TARGET, 10, 0.3);
#
#def cluster_two_diamonds():
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_TWO_DIAMONDS, 5, 0.3);
#
#def cluster_wing_nut():
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_WING_NUT, 1, 1);
#    
#def cluster_chainlink():
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_CHAINLINK);
#    
#def cluster_hepta():
#    template_clustering(7, FCPS_SAMPLES.SAMPLE_HEPTA); 
#    
#def cluster_tetra():
#    template_clustering(4, FCPS_SAMPLES.SAMPLE_TETRA);    
#    
#def cluster_engy_time():
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_ENGY_TIME, 50, 0.5);
#
#def cluster_golf_ball():
#    template_clustering(1, FCPS_SAMPLES.SAMPLE_GOLF_BALL); 
#    
#def cluster_atom():
#    "Impossible to obtain parameters that satisfy us, it seems to me that compression = 0.2 is key parameter here, because results of clustering doesn't depend on number of represented points, except 0."
#    "Thus the best parameters is following: number of points for representation: [5, 400]; compression: [0.2, 0.204]"
#    "Results of clustering is not so dramatically, but clusters are not allocated properly"
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_ATOM, 20, 0.2);
#
#
#def experiment_execution_time(draw, ccore):
#    template_clustering(3, FCPS_SAMPLES.SAMPLE_LSUN, 5, 0.3, draw, ccore);
#    template_clustering(6, FCPS_SAMPLES.SAMPLE_TARGET, 10, 0.3, draw, ccore);
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_TWO_DIAMONDS, 5, 0.3, draw, ccore); 
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_WING_NUT, 1, 1, draw, ccore);
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_CHAINLINK, 5, 0.5, draw, ccore);
#    template_clustering(4, FCPS_SAMPLES.SAMPLE_TETRA, 5, 0.5, draw, ccore);
#    template_clustering(7, FCPS_SAMPLES.SAMPLE_HEPTA, 5, 0.5, draw, ccore);
#    template_clustering(2, FCPS_SAMPLES.SAMPLE_ATOM, 20, 0.2);
#
#
#cluster_sample1();
#cluster_sample2();
#cluster_sample3();
#cluster_sample4();
#cluster_sample5();
#cluster_sample6();
#cluster_elongate();
#cluster_lsun();
#cluster_target();
#cluster_two_diamonds();
#cluster_wing_nut();
#cluster_chainlink();
#cluster_hepta();
#cluster_tetra();
#cluster_atom();
#cluster_engy_time();
#cluster_golf_ball();
#
#
#experiment_execution_time(True, False);
#experiment_execution_time(True, True);#