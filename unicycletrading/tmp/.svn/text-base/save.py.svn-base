#!/usr/bin/env python
import numpy as np
import time
import pylab
import sys
import VectorSet
from Signal_Model import zscore, zscale, ema, argfind, argcnt, argtake, lshift, rshift
import Signal_Model

class My_Model(Signal_Model.new):
    def foo(self):
        pass

vs = VectorSet.new(config_filename=sys.argv[1], signal_class=My_Model, verbose=False)

# print argfind(vs.get_day_mask())
# print vs.get_shifted_day_mask(lsteps=1, rsteps=1)

# this assigns the ticker symbol as a global variable in this scope
for key, value in vs.get_signals().iteritems():
    globals()[key] = value

# print pow(2,32) - 1

# print XLNX.close > XLNX.open
# for ii in range(len(XLNX.binary_pos)):
#     print "0x%x" % XLNX.binary_pos[ii]
# exit(0)

pstep = 100

# for ema_val in range(10,0,-1):
for ema_val in range(10,11):

    predictor = XLNX.midpt - rshift(XLNX.midpt)
    # predictor = ema(XLNX.midpt - rshift(XLNX.midpt), ema_val / 10.0)

    for jj in range(1,40,4):


        # remove the first jj items
        resp_shift = lshift(XLNX.midpt, steps=jj)
        response = resp_shift - np.resize(XLNX.midpt, len(resp_shift))

        # truncate both arrays to the shorter length and divisible by pstep
        aLength = divmod(min(len(predictor), len(response)), pstep)[0] * pstep
        predictor.resize(aLength)
        response.resize(aLength)

        # filter out end/beginning of day
        validmask = ~vs.get_shifted_day_mask(lsteps=jj, rsteps=1)
        validmask.resize(aLength)
        fpred = argtake(predictor, validmask)
        fresp = argtake(response, validmask)
        flength = len(fpred)

        # sort response by predictor, highest to lowest
        ind = fpred.argsort().tolist()
        ind.reverse()
        RESP = np.take(fresp, ind)

        totalHits = float(argcnt(RESP > 0))
        step = flength / pstep
        print step

        cumHits = 0
        lift = list()

        for ii in range(pstep):
            RESPii = np.array(RESP[ii*step:(ii+1)*step])
            cumHits +=  argcnt(RESPii > 0)
            lift.append((cumHits / totalHits) / (float(ii + 1) / pstep))

        pylab.plot(lift, label="steps: "+str(jj)+" ema: "+str(ema_val))

pylab.legend()
pylab.show()

