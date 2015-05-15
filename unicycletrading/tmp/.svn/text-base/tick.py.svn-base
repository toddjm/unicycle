#!/usr/bin/env python
import numpy as np
import time
import pylab
import sys
import VectorSet
from Signal_Model import cma, zscore, zscale, ema, argcnt, lshift, rshift
import Signal_Model

class My_Model(Signal_Model.new):
    def foo(self):
        pass

vs = VectorSet.new(config_filename=sys.argv[1], signal_class=My_Model, verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in vs.get_signals().iteritems():
    globals()[key] = value

def tick(symbol):
    if len(symbol) == 0:
        return 0
    val = [0]
    for ii in range(1, len(symbol)):
        val.append(cmp(symbol[ii], symbol[ii-1]))
    return np.array(val)

x = tick(XLNX.midpt)
up = 0
down = 0
for ii in range(len(x)):
    if (x[ii] == 1):
        up += 1
    elif (x[ii] == -1):
        down += 1
print up, down
