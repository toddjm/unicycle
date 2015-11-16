#!/usr/bin/env python
"""Kolmogorov-Smirnov two-sided test"""

import math
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy

x = numpy.array([
    [-0.15],
    [8.60],
    [5.00],
    [3.71],
    [4.29],
    [7.74],
    [2.48],
    [3.25],
    [-1.15],
    [8.38]
    ])

y = numpy.array([
    [2.55],
    [12.07],
    [0.46],
    [0.35],
    [2.69],
    [-0.94],
    [1.73],
    [0.73],
    [-0.35],
    [-0.37]
    ])


# Kolmogorov-Smirnov two-sided two-sample test

def ks_signif(x):
    """Return smallest significance level for rejecting null hypothesis.

    Use the asymptotic limiting case to compute Q(x) for
    x > 0.
    """
    if (x <= 0.0):
        return 0.0
    v = 0.0
    for i in range(-20, 20):
        v += math.pow(-1.0, i) * math.exp(-2.0 * i ** 2 * x ** 2)
    return (1.0 - v)

def signif(s):
    if (s <= 0.0):
        return 0.0
    v = 0.0
    for ii in range(-10, 10):
        v += math.pow(-1.0, ii) * math.exp(-2.0 * ii**2 * s**2)
    return (1.0 - v)

def gcd(a, b):
    while b:
        a, b = b, a%b
    return a

def ks(x, y, plot=True):
    """Kolmogorov-Smirnov two-sided, two-sample test.

    Returns tuple of the test statistic for arrays `x` and `y` and
    the significance level for rejecting the null hypothesis.
    The empirical distribution functions F(t) and G(t) are
    computed and (optionally) plotted.
    """
    # m, n = # of rows in each array x, y
    m = x.shape[0]
    n = y.shape[0]

    # compute GCD for (m, n)
    d = float(gcd(m, n))

    # flatten, concatenate and sort all data from low to high
    Z = numpy.concatenate((x.flatten(), y.flatten()))
    Z = numpy.sort(Z)

    # ECDFs evaluated at ordered combined sample values Z
    F = numpy.zeros(len(Z))
    G = numpy.zeros(len(Z))

    # compute J
    J = 0.0
    for i in range(len(Z)):
        for j in range(m):
            if (x[j] <= Z[i]):
                F[i] += 1
        F[i] /= float(m)
        for j in range(n):
            if (y[j] <= Z[i]):
                G[i] += 1
        G[i] /= float(n)
        j_max = numpy.abs(F[i] - G[i])
        J = max(J, j_max)
    J *= m * n / d
    # the large-sample approximation
    J_star = J * d / numpy.sqrt(m * n * (m + n))

    if plot:
        y = numpy.arange(m + n) / float(m + n)
        plt.plot(F, y, 'm-', label='F(t)')
        plt.plot(G, y, 'b-', label='G(t)')
        plt.xlabel('value')
        plt.ylabel('cumulative probability')
        mpl.rcParams['legend.loc'] = 'best'
        plt.legend()
        plt.text(0.05, 0.05,
                 'Copyright 2011 bicycle trading, llc',
                 fontsize=15, color='gray', alpha=0.6)
        plt.show()
    return '{0:.4f} {1:.4f}'.format(J_star, ks_signif(J_star))

print ks(x, y)

# m, n = # of rows of data in each array
m = x.shape[0]
n = y.shape[0]

# compute greatest common divisor of (m, n)
d = float(gcd(m, n))

# flatten, concatenate, and sort data low to high
Z_t = numpy.concatenate((x.flatten(), y.flatten()))
Z_t = numpy.sort(Z_t)

# empirical distribution functions evaluated
# at the ordered combined sample values Z_t
F_t = numpy.zeros(len(Z_t))
G_t = numpy.zeros(len(Z_t))

# two-sided, two-sample K-S test statistic
J = 0.0
for ii in range(len(Z_t)):
    for jj in range(m):
        if (x[jj] <= Z_t[ii]):
            F_t[ii] += 1
    F_t[ii] /= float(m)
    for jj in range(n):
        if (y[jj] <= Z_t[ii]):
            G_t[ii] += 1
    G_t[ii] /= float(n)
    J_max = numpy.abs(F_t[ii] - G_t[ii])
    J = max(J, J_max)
J *= m * n / d
J_star = J * d / numpy.sqrt(m * n * (m + n))

# output
print 'p-value = {0:.4f} and alpha = {1:.4f}'.format(J_star, signif(J_star)) 

# for plotting ecdf and cdfs
x1 = F_t
x2 = G_t
y = numpy.arange(m + n) / float(m + n)
plt.plot(x1, y, 'm-', label='F_t')
plt.plot(x2, y, 'b-', label='G_t')
plt.xlabel('value')
plt.ylabel('cumulative probability')
mpl.rcParams['legend.loc'] = 'best'
plt.legend()
plt.text(0.05, 0.05,
         'Copyright 2011 bicycle trading, llc',
         fontsize=15, color='gray', alpha=0.6)
plt.show()
