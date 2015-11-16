#!/usr/bin/env python
"""Kolmogorov-Smirnov two-sided test"""

import math
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy
from mod_analytics import lift, ks

x = numpy.array([
    -0.15,
    8.60,
    5.00,
    3.71,
    4.29,
    7.74,
    2.48,
    3.25,
    -1.15,
    8.38
    ])

y = numpy.array([
    2.55,
    12.07,
    0.46,
    0.35,
    2.69,
    -0.94,
    1.73,
    0.73,
    -0.35,
    -0.37
    ])

lift(x, y, 0.0, 0.0)

print ks(x, y)
