import random
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm


class KMeansHamming:
    """
    K means with Hamming distance 
    """
    def __init__(self, k, x=None, n=0):
        """
        Initialisation 
        :param k: Number of clusters
        :param x: Data points
        :param n: Number of points to generate if x is empty
        """
        self.K = k
        if x == None:
            if n == 0:
                raise Exception("If no data is provided, a parameter N (number of points) is needed")
            else:
                self.N = n
                self.X = self._init_board_gauss(n, k)
        else:
            self.X = x
            self.N = len(x)
        self.mu = None
        self.clusters = None
        self.method = None

    @staticmethod
    def _init_board_gauss(n, k):
        """
        Generation fake data from [-1,1] if not provided. 
        :param n: 
        :param k: 
        :return: 
        """
        n = float(n) / k
        X = []
        for i in range(k):
            c = (random.uniform(-1, 1), random.uniform(-1, 1))
            s = random.uniform(0.05, 0.15)
            x = []
            while len(x) < n:
                a, b = np.array([np.random.normal(c[0], s), np.random.normal(c[1], s)])
                # Continue drawing points from the distribution in the range [-1,1]
                if abs(a) and abs(b) < 1:
                    x.append([a, b])
            X.extend(x)
        X = np.array(X)[:n]
        return X

    def plot_board(self):
        X = self.X
        fig = plt.figure(figsize=(5, 5))
        plt.xlim(-1, 1)
        plt.ylim(-1, 1)
        if self.mu and self.clusters:
            mu = self.mu
            clus = self.clusters
            K = self.K
            for m, clu in clus.items():
                cs = cm.spectral(1. * m / self.K)
                plt.plot(mu[m][0], mu[m][1], 'o', marker='*', \
                         markersize=12, color=cs)
                plt.plot(zip(*clus[m])[0], zip(*clus[m])[1], '.', \
                         markersize=8, color=cs, alpha=0.5)
        else:
            plt.plot(zip(*X)[0], zip(*X)[1], '.', alpha=0.5)
        if self.method == '++':
            tit = 'K-means++'
        else:
            tit = 'K-means with random initialization'
        pars = 'N=%s, K=%s' % (str(self.N), str(self.K))
        plt.title('\n'.join([pars, tit]), fontsize=16)
        plt.savefig('kpp_N%s_K%s.png' % (str(self.N), str(self.K)), \
                    bbox_inches='tight', dpi=200)

    def _cluster_points(self):
        mu = self.mu
        clusters = {}
        for x in self.X:
            bestmukey = min([(i[0], np.linalg.norm(x - mu[i[0]])) \
                             for i in enumerate(mu)], key=lambda t: t[1])[0]
            try:
                clusters[bestmukey].append(x)
            except KeyError:
                clusters[bestmukey] = [x]
        self.clusters = clusters

    def _reevaluate_centers(self):
        clusters = self.clusters
        newmu = []
        keys = sorted(self.clusters.keys())
        for k in keys:
            newmu.append(np.mean(clusters[k], axis=0))
        self.mu = newmu

    def _has_converged(self):
        K = len(self.oldmu)
        return (set([tuple(a) for a in self.mu]) == set([tuple(a) for a in self.oldmu])
                and len(set([tuple(a) for a in self.mu])) == K)

    def find_centers(self, method='random'):
        self.method = method
        x = self.X
        k = self.K
        self.oldmu = random.sample(x, k)
        if method != '++':
            # Initialize to k random centers
            self.mu = random.sample(x, k)
        while not self._has_converged():
            self.oldmu = self.mu
            # Assign all points in x to clusters
            self._cluster_points()
            # Reevaluate centers
            self._reevaluate_centers()

kmeans = KMeansHamming(10, n=200)
kmeans.find_centers()
kmeans.plot_board()