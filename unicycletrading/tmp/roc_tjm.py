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

# predictor: SPY.midpt[t] - SPY.midpt[t-1]
rsteps = 1
predictorShift = rshift(SPY.midpt, steps=rsteps)
predictor = SPY.midpt - predictorShift

# target: SPY.midpt[t+1] - SPY.midpt[t]
lsteps = 1
targetShift = lshift(SPY.midpt, steps=lsteps)
target = targetShift - SPY.midpt

# mask predictor and target
validmask = ~vs.get_shifted_day_mask(lsteps=lsteps, rsteps=rsteps)
predictor = argtake(predictor, validmask)
target = argtake(target, validmask)

# concatenate predictor and target
data = numpy.column_stack((predictor, target))

# observations = # of rows of data
nobv = data.shape[0]

# sort data on predictor (column 0), high to low
idx = data[:,0].argsort()[::-1]
data = data[idx]

# empirical cumulative distribution function
# order the target (column 1) from low to high
ecdf = numpy.sort(data[:,1])

# lift for buy target
buy_threshold = 0.0
liftBuy = numpy.where(data[:,1] > buy_threshold, 1.0, 0.0).cumsum()

# lift for sell target
sell_threshold = 0.0
liftSell = numpy.where(data[:,1][::-1] < sell_threshold, 1.0, 0.0).cumsum()

# average lift = (# of targets in class 1) / nobv
avgLiftBuy = liftBuy[-1] / float(nobv)
avgLiftSell = liftSell[-1] / float(nobv)

# cumulative average lift
cumAvgLiftBuy = numpy.array([avgLiftBuy] * nobv).cumsum()
cumAvgLiftSell = numpy.array([avgLiftSell] * nobv).cumsum()

# normalize lift by subtracting cumulative average lift
normalizedLiftBuy = liftBuy - cumAvgLiftBuy
normalizedLiftSell = liftSell - cumAvgLiftSell

# ROC curve
# total number of P and N in target
theta = buy_threshold
totalP = numpy.where(data[:,1] >= theta, 1.0, 0.0).sum()
totalN = numpy.where(data[:,1] < theta, 1.0, 0.0).sum()

# true P and false P rates
TPR = numpy.logical_and(numpy.where(data[:,0] >= theta, 1.0, 0.0),
                       numpy.where(data[:,1] >= theta, 1.0, 0.0)).astype(float)
FPR = numpy.logical_and(numpy.where(data[:,0] >= theta, 1.0, 0.0),
                       numpy.where(data[:,1] < theta, 1.0, 0.0)).astype(float)

# true P and false P
TP = TPR.sum()
FP = FPR.sum()

# false N and true N
FN = totalP - TP
TN = totalN - FP

# report as fractions
TPF = TP / totalP
FPF = FP / totalN

# P and N diagnostic likelihood ratios
DLRP = TPF / FPF
DLRN = (1.0 - TPF) / (1.0 - FPF)

print 'TN = ', TN, 'FN = ', FN
print 'FP = ', FP, 'TP = ', TP
print 'sensitivity = ', TPF
print 'specificity = ', 1.0 - FPF
print 'rho = ', TP + FN
print 'tau = ', FP + TP
print 'PPV = ', TP / (FP + TP)
print 'NPV = ', TN / (TN + FN)
print 'DLRP = ', DLRP
print 'DLRN = ', DLRN

# for plotting ECDF
x = numpy.sort(data[:,1])
x1 = numpy.sort(numpy.random.normal(0.0, 0.015,nobv))
x2 = numpy.sort(numpy.random.uniform(numpy.min(x), numpy.max(x), nobv))
y = numpy.arange(nobv) / float(nobv)
plt.plot(x, y, 'b-')
plt.plot(x1, y, 'r-')
plt.plot(x2, y, 'g-')
plt.xlabel('target')
plt.ylabel('CDF')
plt.show()

n, bins, patches = plt.hist(numpy.sort(data[:,1]), bins=51, range=(-0.15,0.15), normed=True)
y = mlab.normpdf(bins, 0.0, 0.015)
plt.plot(bins, y, 'r--', linewidth=1)
plt.show()

# for plotting ROC curve
x = FPR.cumsum() / FP
y = TPR.cumsum() / TP
# area under ROC curve
AUC = 0.0
for ii in range(1, nobv):
    AUC += (x[ii] - x[ii-1]) * (y[ii] + y[ii-1]) / 2.0
print 'AUC = ', AUC
plt.plot(x, y, 'b-')
plt.plot(numpy.arange(2), 'r-')
plt.xlabel('FPF')
plt.ylabel('TPF')
#plt.show()

# for plotting lift
figLift = plt.figure()
p1 = figLift.add_subplot(111)
x1 = numpy.arange(nobv)
y1 = normalizedLiftBuy
p1.plot(x1, y1, 'b-')
p1.grid(which='both')
p1.minorticks_on()
p1.set_xlabel('observations')
p1.set_ylabel('lift - buy (normalized)', color='b')
for tick in p1.get_yticklabels():
    tick.set_color('b')

p2 = p1.twinx()
y2 = data[:,0]
p2.plot(x1, y2, 'r-')
p2.set_ylabel('predictor', color='r')
for tick in p2.get_yticklabels():
    tick.set_color('r')
plt.show()
