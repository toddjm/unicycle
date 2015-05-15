#!/usr/bin/env python
import math
import matplotlib.axis
import matplotlib.pyplot as plt
import numpy
import sys
import VectorSet
#import Signal_Model
import Signal
from signal_types import lshift, rshift, argtake, zscale, ema
from signal_statistics import lift


class MyModel(Signal.Signal):
    pass

dataSet = VectorSet.new(config_filename=sys.argv[1],
                        signal_class=MyModel,
                        verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in dataSet.get_signals().iteritems():
    globals()[key] = value


bars = 10

# predictor: SPY.midpt[t] - SPY.midpt[t-1]
rsteps = bars
predictorShift = rshift(SPY.obv, steps=rsteps)
predictor = SPY.obv - predictorShift
#
# target: SPY.midpt[t+1] - SPY.midpt[t]
lsteps = 25
targetShift = lshift(SPY.midpt, steps=lsteps)
target = targetShift - SPY.midpt
#
## mask predictor and target
validmask = ~dataSet.get_shifted_day_mask(lsteps=lsteps, rsteps=rsteps)
predictor = argtake(predictor, validmask)
target = argtake(target, validmask)

plt.plot(ema(predictor, bars))
plt.show()
#
#lift(predictor, target, 0.0, 0.0)
#
## concatenate predictor and target
#data = numpy.column_stack((predictor, target))
#
## observations = # of rows of data
#nobv = data.shape[0]
#
## sort data on predictor (column 0), high to low
#idx = data[:, 0].argsort()[::-1]
#data = data[idx]
#
## lift for buy target
#buy_threshold = 0.0
#liftBuy = numpy.where(data[:, 1] > buy_threshold, 1.0, 0.0).cumsum()
#
## lift for sell target
#sell_threshold = buy_threshold
#liftSell = numpy.where(data[:, 1][::-1] < sell_threshold, -1.0, 0.0).cumsum()
#
## mean lift = (total # of targets in class 1) / nobv
#meanLiftBuy =  liftBuy[-1] / float(nobv)
#meanLiftSell = liftSell[-1] / float(nobv)
#
## cumulative mean lift
#cumMeanLiftBuy = numpy.array([meanLiftBuy] * nobv).cumsum()
#cumMeanLiftSell = numpy.array([meanLiftSell] * nobv).cumsum()
#
## normalize lift by subtracting the cumulative mean lift
#normalizedLiftBuy = liftBuy - cumMeanLiftBuy
#normalizedLiftSell = liftSell - cumMeanLiftSell
#
## output:
## buy or sell:
## percent of ordered observations
## raw lift
## ratio of lift to mean lift for the percentile
## number of samples in the percentile
#
#bins = numpy.floor(numpy.sqrt(nobv) + 0.5).astype(int)
#step = nobv / bins
#idx = [numpy.floor(float(ii) / nobv * 100.0 + 0.5) for ii in range(1, nobv+1)]
#rawLiftBuy = numpy.insert(numpy.array([liftBuy[ii] /
#                                       float(ii) for ii in range(1, nobv)]), 0, 0.0)
#rawLiftSell = numpy.insert(numpy.array([liftSell[ii] /
#                                        float(ii) for ii in range(1, nobv)]), 0, 0.0)
#
##print 'buy:'
##for ii in range(step-1, nobv, step):
##    print "%5.2f%% %4.3f %4.3f %d" % (idx[ii],
##                                      rawLiftBuy[ii],
##                                      100.0 * ((rawLiftBuy[ii] / meanLiftBuy) - 1.0) / idx[ii],
##                                      ii)
#
##print 'sell:'
##for ii in range(step-1, nobv, step):
##    print "%5.2f%% %4.3f %4.3f %d" % (idx[ii],
##                                      rawLiftSell[ii],
##                                      100.0 * ((rawLiftSell[ii] / meanLiftSell) - 1.0) / idx[ii],
##                                      ii)
#
#
## empirical cumulative distribution function
## order the target (column 1), low to high
#ecdf = numpy.sort(data[:, 1])
#
## compute basic statistics
#ecdfMean = numpy.mean(ecdf)
#ecdfMedian = numpy.median(ecdf)
#ecdfStd = numpy.std(ecdf)
#ecdfVar = numpy.var(ecdf)
#ecdfHist, ecdfBinEdges = numpy.histogram(ecdf, bins=numpy.arange(100), normed=True)
#
#print ecdfMean, ecdfMedian, ecdfStd, ecdfVar

# plot normalized lift for buy target
#fig = plt.figure()
#p1 = fig.add_subplot(111)
#x1 = numpy.arange(nobv)
#y1 = normalizedLiftBuy
#p1.plot(x1, y1, 'b-')
#p1.grid(which='both')
#p1.minorticks_on()
#p1.set_xlabel('observations')
#p1.set_ylabel('normalized lift - buy', color='b')
#for tick in p1.get_yticklabels():
#    tick.set_color('b')
##p1.tick_params(axis='y', color='b', width=2)
#p2 = p1.twinx()
#y2 = data[:, 0]
#p2.plot(y2, 'b-')
#
## plot normalized lift for sell target
#p3 = p1.twinx()
#y3 = normalizedLiftSell
#p3.plot(y3, 'r-')
#p3.set_ylabel('normalized lift - sell', color='r')
#for tick in p3.get_yticklabels():
#    tick.set_color('r')
##p3.tick_params(axis='y', color='r', width=2)
#p4 = p1.twinx()
#y4 = data[:, 0][::-1]
#p4.plot(y4, 'r-')
#yticklabels = p2.get_yticklabels() + p4.get_yticklabels()
#yticklines = p2.get_yticklines() + p4.get_yticklines()
#plt.setp(yticklabels, visible=False)
#plt.setp(yticklines, visible=False)
##plt.show()

# plot ECDF
#random = numpy.random.randn(nobv)
#idx = random.argsort()
#random = zscale(random[idx])
#ecdf = zscale(ecdf)
#yaxis = numpy.arange(nobv) / float(nobv)
#fig = plt.figure()
#p1 = fig.add_subplot(111)
#p1.plot(SPY.obv, yaxis, 'b-')
#p1.grid(which='both')
#p1.minorticks_on()
#p1.set_xlabel('scaled value')
#p1.set_ylabel('cumulative probability')
#for tick in p1.get_yticklabels():
#    tick.set_color('b')
#p2 = p1.twinx()
#p2.plot(random, yaxis, 'r-')
#print "At end of plot ECDF"
#plt.show()
