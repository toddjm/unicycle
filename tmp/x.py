#!/usr/bin/env python
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import numpy
import sys
import mod_vectorset
import mod_signal
from mod_signal import lshift, rshift, argtake

class MyModel(mod_signal.new):
    pass

vs = mod_vectorset.new(config_filename=sys.argv[1],
                       signal_class=MyModel,
                       verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in vs.get_signals().iteritems():
    globals()[key] = value
    print key

for i in range(len(SPY.close)):
    print SPY.close[i]
