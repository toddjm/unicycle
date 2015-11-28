#!/usr/bin/env python
"""Kolmogorov-Smirnov two-sided test"""

import math
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from mod_analytics import lift, ks

x = np.array([
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

y = np.array([
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

x = np.random.randn(1000, )
y = np.random.randn(1000, )
lift(x, y, 0.0, 0.0)

print ks(x, y)
