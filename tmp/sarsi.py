#!/usr/bin/env python
"""sarsi computation."""

from collections import deque
import matplotlib as mpl
import matplotlib.axis
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import numpy
import sys
import mod_vectorset
import mod_signal
from mod_signal import lshift, rshift, argtake, zscale
from mod_analytics import lift, ks

class MyModel(mod_signal.new):
    pass

dataSet = mod_vectorset.new(config_filename=sys.argv[1],
                            signal_class=MyModel,
                            verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in dataSet.get_signals().iteritems():
    globals()[key] = value

# remove SPY from dataSet; use new length of dataSet for duration
SPY = dataSet.get_signals().pop('SPY')
duration = len(SPY.close)

# compute SARSI
period = 10
SARSI = numpy.zeros(duration)
modSARSI = numpy.zeros(duration)
# loop over symbols
for symbol in dataSet.get_signals().itervalues():
    temp = deque([0.0] * period)
    val = 0.0
    dn = 0.0
    summ = 0.0
    # loop over time steps
    for ts in range(duration):
        sv = symbol.close[ts]
        temp.append(sv)
        summ += sv - temp.popleft()
        dn = min(dn+1, period)
        val = summ / dn
        modSARSI[ts] += cmp(sv, val)
#        if (sv > val):
#            SARSI[ts] += 1.0
modSARSI /= float(len(dataSet.get_signals().keys()))

# predictor: SARSI[t] - SARSI[t-1]
rsteps = 1
predictorShift = rshift(modSARSI, steps=rsteps)
predictor = modSARSI - predictorShift

# target: SPY.midpt[t+1] - SPY.midpt[t]
lsteps = 1
targetShift = lshift(SPY.close, steps=lsteps)
target = targetShift - SPY.close

# mask predictor and target
validmask = ~dataSet.get_shifted_day_mask(lsteps=lsteps, rsteps=rsteps)
predictor = argtake(predictor, validmask)
target = argtake(target, validmask)

x = numpy.column_stack((predictor,target))
numpy.savetxt('sarsi_spy_close_15min.out', x)

lift(predictor[period:], target[period:])

nobv = duration - period
    
# ROC
# convention here is class P if >= and class F if <, per Pepe 2003.

# threshold
theta = 0.0

# total number of positives (P) and negatives (N) in target
P = numpy.where(target[period:] >= theta, 1.0, 0.0).sum()
N = numpy.where(target[period:] < theta, 1.0, 0.0).sum()

# true P and false P rates
TPR = numpy.logical_and(numpy.where(predictor[period:] >= theta, 1.0, 0.0),
                        numpy.where(target[period:] > 0.0, 1.0, 0.0)).astype(float)
FPR = numpy.logical_and(numpy.where(predictor[period:] >= theta, 1.0, 0.0),
                        numpy.where(target[period:] < 0.0, 1.0, 0.0)).astype(float)

# TP, FP, FN, TN
TP = TPR.sum()
FP = FPR.sum()
FN = P - TP
TN = N - FP

# report as fractions
TPF = TP / (TP + FN)
FPF = FP / N
FNF = 1 - TPF
TNF = TN / (TN + FP)

# diagnostic likelihood ratios
DLRP = TPF / FPF
DLRN = FNF / TNF

# probability of a positive prediction
tau = (FP + TP) / (P + N)

# probability of a positive target
rho = (TP + FN) / (P + N)

# positive predictive value
PPV = TP / (TP + FP)

# negative predictive value
NPV = TN / (TN + FN)

# output for ROC

#print 'max of predictor = ', max(predictor[period:])
#print 'min of predictor = ', min(predictor[period:])

print 'threshold = ', theta
print
print 'Classification probabilities on [0, 1]'
print 'FPF: {0:.3f} TPF: {1:.3f} tau: {2:.3f}'.format(FPF, TPF, tau)
print
print 'Predictive values on [0, 1]'
print 'PPV: {0:.3f} NPV: {1:.3f} rho: {2:.3f}'.format(PPV, NPV, rho)
print
print 'Diagnostic likelihood ratios on [0, +infty)'
print 'DLR+: {0:.3f} DLR-: {1:.3f}'.format(DLRP, DLRN)

#AUC = 0.0
#x = FPR.cumsum() / FP
#y = TPR.cumsum() / TP
#for ii in range(1, nobv):
#    AUC += (x[ii] - x[ii-1]) * (y[ii] + y[ii-1]) / 2.0
#print 'AUC = ', AUC

# empirical cumulative distribution function
# order the predictor (column 1) and
# target (column 1), low to high
#ecdfPredictor = numpy.sort(predictor[period:])
#ecdfTarget = numpy.sort(target[period:])

# for plotting ecdf and cdfs
#x1 = ecdfPredictor
#x2 = ecdfTarget
#x3 = zscale(numpy.sort(numpy.random.normal(mu, sigma, nobv)))
#x4 = zscale(numpy.sort(numpy.random.logistic(mu, sigma, nobv)))
#y = numpy.arange(nobv) / float(nobv)
#plt.plot(x1, y, 'm-', label='predictor ECDF')
#plt.plot(x2, y, 'b-', label='target ECDF')
#plt.plot(x3, y, 'r-', label='random normal CDF')
#plt.plot(x4, y, 'g-', label='random logistic CDF')
#plt.xlabel('value')
#plt.ylabel('cumulative probability')
#mpl.rcParams['legend.loc'] = 'best'
#plt.legend()
#plt.text(-0.95, 0.05,
#         'Copyright 2011 bicycle trading, llc',
#         fontsize=15, color='gray', alpha=0.5)
#plt.show()

# for plotting histogram and normal PDF for target
#nbins = nobv / 10
#pdf, bins, patches = plt.hist(numpy.sort(target), bins=nbins, normed=True)
#print numpy.sum(pdf * numpy.diff(bins))
#mu = numpy.mean(target)
#sigma = numpy.std(target)
#y = mlab.normpdf(bins, mu, sigma)
#plt.plot(bins, y, 'r--', linewidth=1)
#plt.show()

# for plotting ROC curve
#x = FPR.cumsum() / FP
#y = TPR.cumsum() / TP
#plt.plot(x, y, 'b-')
#plt.plot(numpy.arange(2), 'r-')
#plt.xlabel('FPF')
#plt.ylabel('TPF')
#plt.show()

# for plotting cross-correlation of predictor and target
fig = plt.figure()
ax1 = fig.add_subplot(311)
x = predictor[period:]
y = target[period:]
ax1.xcorr(x, y, usevlines=True, maxlags=50, normed=True, lw=2)
ax1.grid(True)
ax1.axhline(0, color='black', lw=2)

ax2 = fig.add_subplot(312, sharex=ax1)
ax2.acorr(x, usevlines=True, normed=True, maxlags=50, lw=2)
ax2.grid(True)
ax2.axhline(0, color='black', lw=2)

ax3 = fig.add_subplot(313, sharex=ax1)
ax3.acorr(y, usevlines=True, normed=True, maxlags=50, lw=2)
ax3.grid(True)
ax3.axhline(0, color='black', lw=2)

plt.show()
