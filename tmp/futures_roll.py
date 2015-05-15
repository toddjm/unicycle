#!/usr/bin/env python
import matplotlib.pyplot as plt
import numpy
import sys
import mod_vectorset
import mod_signal
from mod_signal import lshift, rshift, argtake, zscale, sma

class MyModel(mod_signal.new):
    pass

def dydt(s):
    v = numpy.zeros(len(s))
    for ii in range(1, len(s)):
        v[ii] = s[ii] - s[ii-1]
    return v


dataSet = mod_vectorset.new(config_filename=sys.argv[1],
			signal_class=MyModel,
			verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in dataSet.get_signals().iteritems():
    globals()[key] = value

period = len(HO201003.midpt)

x = HO201003.volume.astype(float)
avgx = sma(HO201003.volume.astype(float), 2)

y = HO201004.volume.astype(float)
avgy = sma(HO201004.volume.astype(float), 2)

dx = dydt(x)
dy = dydt(y)

for ii in range(len(x)):
    if (y[ii] / x[ii] > 1.0 and x[ii] < avgx[ii] and y[ii] > avgy[ii]):        
        print 'yesterday = ', ii-1, 'vol (current contract) = ', x[ii-1],\
        'vol (next contract) = ', y[ii-1]
        print 'today = ', ii, 'vol (current contract) = ', x[ii],\
        'vol (next contract) = ', y[ii]
        print 'roll is end of day ', ii

