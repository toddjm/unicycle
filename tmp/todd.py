#!/usr/bin/env python
from collections import deque
import numpy
import pylab
import sys
import mod_vectorset
import mod_signal
from mod_signal import zscale, lshift, rshift, argtake, argcnt, argfind

class MyModel(mod_signal.new):
        pass

dataSet = mod_vectorset.new(config_filename=sys.argv[1],
			signal_class=MyModel,
			verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in dataSet.get_signals().iteritems():
    globals()[key] = value


print type(SPY.close)
print len(SPY.midpt)
# define predictor as 1-bar delta SPY.midpt
rsteps = 1
predictorShift = rshift(SPY.midpt, steps=rsteps)
predictor = SPY.midpt - predictorShift

# define target as 1-bar delta SPY.midpt buy and truncate by lsteps
lsteps = 1
targetShift = lshift(SPY.midpt, steps=lsteps)
target = targetShift - numpy.resize(SPY.midpt, len(targetShift))

# resize predictor and target arrays to shorter length
adj = min(len(predictor), len(target))
predictor.resize(adj)
target.resize(adj)

# mask predictor and target
validmask = ~dataSet.get_shifted_day_mask(lsteps=lsteps, rsteps=rsteps)
validmask.resize(adj)
predictor = argtake(predictor, validmask)
target = argtake(target, validmask)
npoints = len(target)
fnpoints = float(npoints)

# sort predictor from high to low, sort target on those indices
idx = predictor.argsort()[::-1]
predictorSorted = numpy.take(predictor, idx)
targetSorted = numpy.take(target, idx)

# define threshold and apply to targetSorted
threshold = 0.0
targetSorted = numpy.where(targetSorted > threshold, 1.0, 0.0)

# lift = cumulative sum of class P as we go from
# 0% of total sample to 100% of total sample population
lift = list()
cnt = 0
for ii in range(npoints):
	cnt += targetSorted[ii]
	lift.append(cnt)
lift = numpy.array(lift)

# average lift = # class P in total sample / total sample size
avgLift = float(cnt) / fnpoints

# cumulative average lift = sum of (average lift * index #) where
# index runs from 0 to npoints
cumAvgLift = numpy.array([avgLift] * npoints)
for ii in range(npoints):
	cumAvgLift[ii] *= ii
	
randomChoice = 0.5 * numpy.arange(0.0, fnpoints)

pylab.plot((lift - cumAvgLift)/fnpoints, label='normalized lift')
#pylab.plot(randomChoice/avgLift, label='random')
#pylab.plot(overall, label='overall')
#pylab.plot(predictorSorted, label='predictor')
pylab.legend()
pylab.grid(True)
pylab.show()
