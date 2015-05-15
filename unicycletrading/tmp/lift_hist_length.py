#!/usr/bin/env python
import numpy as np
import time
import pylab
import sys
import VectorSet
from Signal_Model import zscore, zscale, ema, argfind, argcnt, argtake, lshift, rshift, binary_pos, satcnt_pos
import Signal_Model

class My_Model(Signal_Model.new):
    def foo(self):
        pass

vs = VectorSet.new(config_filename=sys.argv[1], signal_class=My_Model, verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in vs.get_signals().iteritems():
    globals()[key] = value

# pylab.plot(XLNX.close - XLNX.open)
# pylab.show()

# bits=0 totalCount=95418 totalHits=46917
# bits=1 totalCount=24190 totalHits=13058
# bits=2 totalCount=7015 totalHits=3806
# bits=3 totalCount=2268 totalHits=1203
# bits=4 totalCount=771 totalHits=402
# bits=5 totalCount=282 totalHits=155
# bits=6 totalCount=114 totalHits=67

pstep = 100
lsteps = 20

for bb in range(7):

    bits = bb
    bitmask = pow(2,bits) - 1

    # for ii in range(pow(2,bits)):

    # predictor = ema(XLNX.midpt - rshift(XLNX.midpt), 0.75)
    predictor = XLNX.midpt - rshift(XLNX.midpt)
    # predictor = XLNX.close - XLNX.open

    # remove the first lsteps items
    resp_shift = lshift(XLNX.midpt, steps=lsteps)
    response = resp_shift - np.resize(XLNX.midpt, len(resp_shift))
    history = binary_pos(XLNX.close - XLNX.open)
    #     history = binary_pos(predictor)

    # truncate both arrays to the shorter length and divisible by pstep
    aLength = divmod(min(len(predictor), len(response)), pstep)[0] * pstep
    predictor.resize(aLength)
    response.resize(aLength)
    history.resize(aLength)

    satcnt = satcnt_pos(response, history, bb)

    # filter out end/beginning of day
    validmask = ~vs.get_shifted_day_mask(lsteps=lsteps, rsteps=1)
    validmask.resize(aLength)

    #     historymask = (history & bitmask) == ii
    historymask = ((history & bitmask) == bitmask) & (satcnt < 2)
#     historymask = ((history & bitmask) == bitmask)
    validmask = validmask & historymask

    fpred = argtake(predictor, validmask)
    fresp = argtake(response, validmask)
    flength = len(fpred)

    # sort response by predictor, highest to lowest
    ind = fpred.argsort().tolist()
    ind.reverse()
    RESP = np.take(fresp, ind)

    totalHits = float(argcnt(RESP > 0))

    print "bits=%d totalCount=%d totalHits=%d" % (bb, flength, totalHits)

    if not totalHits == 0:
        step = flength / pstep

        cumHits = 0
        lift = list()

        for jj in range(pstep):
            RESPjj = np.array(RESP[jj*step:(jj+1)*step])
            cumHits +=  argcnt(RESPjj > 0)
            lift.append((cumHits / totalHits) / (float(jj + 1) / pstep))

        pylab.plot(lift, label=str(bb))

pylab.legend()
pylab.show()

