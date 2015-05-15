#!/usr/bin/env python
from collections import deque
import math
import matplotlib.axis
import matplotlib.pyplot as plt
import numpy as np
import sys
import VectorSet
import Signal_Model
from Signal_Model import lshift, rshift, argtake, zscale
from Analytics import lift

class MyModel(Signal_Model.new):
    pass

dataSet = VectorSet.new(config_filename=sys.argv[1],
			signal_class=MyModel,
			verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in dataSet.get_signals().iteritems():
    globals()[key] = value

def ema(x, alpha):
    """
    Returns exponential moving average of x with smoothing constant alpha:

    >>> print ema([1, 2, 3, 4, 5], 1.0/3.0)
    [ 1.          1.33333333  1.88888889  2.59259259  3.39506173]

    """
    y = np.zeros(len(x))
    y[0] = x[0]
    for i in range(1, len(x)):
        y[i] = alpha * x[i] + (1.0 - alpha) * y[i-1]
    return y

def obv(x):
    

#print ema(SPY.midpt, 0.5)
print ema(np.arange(1,11), 1.0/3.0)

def rocind(s, n):
    """Returns new array of `s` containing the `n`-period
    rate of change indicator.

    """
    if len(s) == 0:
        return 0
    vals = [0.0] * n
    for ii in range(n, len(s)):
        val = 100.0 * ((s[ii] - s[ii-n]) / s[ii-n])
        vals.append(val)
    return np.array(vals)

def rsi(s, n):
    """Returns new array of `s` containing the `n`-period
    Relative Strength Index (RSI).
      
    """
    if len(s) == 0:
        return 0
    avg_gain = 0.0
    avg_loss = 0.0
    rs = [0.0] * n
    val = 0.0
    for ii in range(1, n+1):
        val = s[ii] - s[ii-1]
        if (val > 0.0):
            avg_gain += val
        elif (val < 0.0):
            avg_loss += abs(val)
    avg_gain /= float(n)
    avg_loss /= float(n)
    rs.append(avg_gain / avg_loss)
    for ii in range(n+1, len(s)):
        val = s[ii] - s[ii-1]
        if (val > 0.0):
            avg_gain = (avg_gain * (n - 1) + val) / float(n)
            avg_loss *= (n - 1) / float(n)
        elif (val < 0.0):
            avg_gain *= (n - 1) / float(n)
            avg_loss = (avg_loss * (n - 1) + abs(val)) / float(n)
        rs.append(avg_gain / avg_loss)
    for ii in range(len(rs)):
        rs[ii] = 100.0 - (100.0 / (1.0 + rs[ii]))
    return np.array(rs)
    
def dpo(s, n):
    """Returns a new array of `s` containing the `n`-period
    detrended price oscillator (DPO).

    """
    if len(s) == 0:
        return 0
    p = (n / 2) + 1
    smaval = sma(s, n)
    vals = [s[n-p] - s[0]]
    cnt = 1
    for ii in range(n, len(s)):
        vals.append(s[ii-p] - smaval[cnt])
        cnt += 1
    return np.array(vals)

def sma(s, n):
    """Returns new array of `s` containing the `n`-period
    simple moving average of a sequence of numbers.

    """
    temp = deque([0.0] * n)
    vals = list()
    dn = 0.0
    summ = 0.0
    for ii in range(len(s)):
        temp.append(s[ii])
        summ += s[ii] - temp.popleft()
        dn = min(dn+1, n)
        vals.append(summ / dn)
    return np.array(vals)

def smooth(x, window_len, window):
    if x.ndim != 1:
        return 0
    if x.size < window_len:
        return 0
    if window_len < 3:
        return 0
    if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        return 0
    s = np.r_[2 * x[0] - x[window_len:1:-1], x, 2 * x[-1] - x[-1:-window_len:-1]]
    if window == 'flat':
        w = np.ones(window_len, 'd')
    else:
        w = eval('np.'+window+'(window_len)')
    y = np.convolve(w / w.sum(), s, mode='same')
    return y[window_len-1:-window_len+1]

def ema(x, alpha):
    y = np.zeros(len(x))
    y[0] = x[0]
    for i in range(1, len(x)):
        y[i] = alpha * x[i] + (1.0 - alpha) * y[i-1]
    return y

#def ema_masked(signal, alpha, mask=None):
#    mask = (signal != signal) if mask == None else mask
#    mask[0] = True
#    val = 0.0
#    vals = list()
#    for ii in range(len(signal)):
#        val = signal[ii] if mask[ii] else (alpha * signal[ii] + (1.0 - alpha) * val)
#        vals.append(val)
#    return np.array(vals)
#
#def ema(signal, alpha):
#    if (type(alpha).__name__ =='int'):
#        alpha = float(2.0 / (alpha + 1.0))
#    return ema_masked(signal, alpha)

def binary_pos(signal):
    val = 0
    vals = list()
    for ii in range(len(signal)):
        if signal[ii] > 0:
            val |= 1
        vals.append(val)
        val &= pow(2, 31) - 1
        val = val << 1
    return np.array(vals)

def satcnt_true(predictor, response, history, bits, satbits=2):
    size = pow(2, bits)
    bitmask = size - 1
    vals = list()
    satcnt = list()
    satmax = pow(2, satbits) - 1
 
    for ii in range(size):
        satcnt.append(0)
 
    for ii in range(len(predictor)):
        index = history[ii] & bitmask
        if predictor[ii]:
            if (response[ii]):
                satcnt[index] = min(satcnt[index] + 1, satmax)
            else:
                satcnt[index] = max(satcnt[index] - 1, 0)
        vals.append(satcnt[index])
#         print "index=%d sig=%s" % (index, signal[ii]),
#         for ii in range(size):
#             print " %d" % (satcnt[ii]),
#         print
    return np.array(vals)

def cma(s):
    """Returns new array of `s` containing the
    cumulative moving average of `s.`

    >>> print cma([10, 20, 30, 40, 50])
    [ 10.  15.  20.  25.  30.]
    """
    if len(s) == 0:
        return 0
    val = 0.0
    vals = list()
    for ii in range(len(s)):
        val = (s[ii] + (ii * val)) / (ii + 1.0)
        vals.append(val)
    return np.array(vals)

def zscore(s):
    return ((s - np.mean(s)) / np.std(s))

def zscale(s):
    """Returns new array of `s` with values on [-1,1].

    """
    return (2.0 * (s - np.min(s)) / (np.max(s) - np.min(s))) - 1.0

def argfind(bool_signal):
    return np.nonzero(bool_signal)[0]

def argcnt(bool_signal):
    return len(argfind(bool_signal))

def argtake(signal, bool_signal):
    return np.take(signal, argfind(bool_signal))

def lshift(signal, steps=1):
    """Return new array of `signal` shifted to the left by `steps` filling the end of the array
    with the last value in `signal` to preserve the original length
    """
    return np.append(np.delete(signal, slice(0, steps)), np.repeat(signal[-1], steps))

def rshift(signal, steps=1, init=None):
    """Return new array of `signal` shifted to the right by `steps` filling the beginning of the array
    with the first value in `signal` (or the value of `init`) to preserve the original length
    """
    init = signal[0] if init == None else init
    return np.resize(np.insert(np.copy(signal), np.zeros(steps), [init]), len(signal))

class ref():
    signal = None
    signals = None

    def __getattr__(self, name):
        return self.signal.__getattr__(name)

    def __getitem__(self, index):
        return self.signals[index]

    def set_signal(self, signal, index=None):
        if index == None:
            self.signal = signal
        else:
            if (self.signals == None):
                self.signals = {}
            self.signals[index] = signal

class new():

    dbh = None
    validSlice = None
    src_tblname = None
    tblname = None
    signalHash = None

    def __init__(self, dbh, validSlice, src_tblname, symbol=None):
        self.dbh = dbh
        self.src_tblname = src_tblname
        self.setValidSlice(validSlice)
        self.symbol = symbol

    def setValidSlice(self, validSlice):
        self.validSlice = validSlice
        self.signalHash = {}

    def __getattr__(self, name):
        return getattr(self, "_"+name)()

    def _open(self):
        return self.get_raw_signal("open")

    def _ts(self):
        return self.get_raw_signal("ts")

    def _close(self):
        return self.get_raw_signal("close")

    def _high(self):
        return self.get_raw_signal("high")

    def _low(self):
        return self.get_raw_signal("low")

    def _volume(self):
        return self.get_raw_signal("volume")

    def _wap(self):
        return self.get_raw_signal("wap")

    def _vwap(self):
        return self.wap

    def _count(self):
        return self.get_raw_signal("count")

    def _midpt(self):
        return (self.high + self.low) / 2.0

    def _range(self):
        return (self.high - self.low)

    def _tprice(self):
        return((self.high + self.low + self.close) / 3.0)

    def _mf(self):
        return(self.tprice * self.volume)

    def _pvt(self):
        if not self.signalHash.has_key('pvt'):
            summ = self.accdist_item(self.volume[0], self.high[0], self.low[0], self.close[0])
            vals = [summ]
            for ii in range(1, len(self.open)):
                summ += self.volume[ii] * ((self.close[ii] - self.close[ii-1]) / self.close[ii-1])
                vals.append(summ)
            self.signalHash['pvt'] = np.array(vals)
        return self.signalHash['pvt']

    def _pdm(self):
        if not self.signalHash.has_key('pdm'):
            vals = [0]
            for ii in range(1, len(self.open)):
                um = self.high[ii] - self.high[ii-1]
                dm = self.low[ii-1] - self.low[ii]
                if (um > dm and um > 0.0):
                    vals.append(um)
                else:
                    vals.append(0.0)
            self.signalHash['pdm'] = np.array(vals)
        return self.signalHash['pdm']

    def _ndm(self):
        if not self.signalHash.has_key('ndm'):
            vals = [0]
            for ii in range(1, len(self.open)):
                um = self.high[ii] - self.high[ii-1]
                dm = self.low[ii-1] - self.low[ii]
                if (dm > um and dm > 0.0):
                    vals.append(dm)
                else:
                    vals.append(0.0)
            self.signalHash['ndm'] = np.array(vals)
        return self.signalHash['ndm']

    def _dmi(self):
        if not self.signalHash.has_key('dmi'):
            vals = [0]
            for ii in range(1, len(self.open)):
                val = (100.0 * abs(self.pdm[ii] - self.ndm[ii])) + 1.0
                val /= (self.pdm[ii] + self.ndm[ii]) + 1.0
                vals.append(val)
            self.signalHash['dmi'] = np.array(vals)
        return self.signalHash['dmi']

    def _tr(self):
        if not self.signalHash.has_key('tr'):
            val = self.midpt[0]
            vals = [val]
            for ii in range(1, len(self.open)):
                val = max(abs(self.high[ii] - self.low[ii]),
                          abs(self.high[ii] - self.close[ii-1]),
                          abs(self.close[ii-1] - self.low[ii]))
                vals.append(val)
            self.signalHash['tr'] = np.array(vals)
        return self.signalHash['tr']
            
    def accdist_item(self, volume, high, low, close):
        return volume * self.clv_item(high, low, close)

    def _accdist(self):
        if not self.signalHash.has_key('accdist'):
            vals = list()
            summ = 0.0
            for ii in range(len(self.close)):
                summ += self.accdist_item(self.volume[ii], self.high[ii], self.low[ii], self.close[ii])
                vals.append(summ)
            self.signalHash['accdist'] = np.array(vals)
        return self.signalHash['accdist']

    def _obv(self):
        if not self.signalHash.has_key('obv'):
            summ = self.accdist_item(self.volume[0], self.high[0], self.low[0], self.close[0]) # initialize to first value of accdist
            vals = [summ]
            for ii in range(1, len(self.close)):
                summ += (self.volume[ii] * cmp(self.close[ii], self.close[ii-1]))
                vals.append(summ)
            self.signalHash['obv'] = np.array(vals)
        return self.signalHash['obv']

    def clv_item(self, high, low, close):
        return 0 if high == low else ((close - low) - (high - close) / (high - low))

    def _clv(self):
        if not self.signalHash.has_key('clv'):
            vals = list()
            for ii in range(len(self.close)):
                vals.append(self.clv_item(self.high[ii], self.low[ii], self.close[ii]))
            self.signalHash['clv'] = np.array(vals)
        return self.signalHash['clv']

    def get_tblname(self):
        if (self.tblname == None):
            self.tblname = self.dbh.get_random_name()
            self.dbh.execute("CREATE TEMPORARY TABLE "+self.tblname+" LIKE "+self.src_tblname)

            if not self.symbol.is_rolled_future():
                self.dbh.execute("INSERT INTO "+ self.tblname +" SELECT r.* from "+self.validSlice.get_tblname()+" l, "+self.src_tblname+" r where l.ts=r.ts and l.valid")
            else:
                roll_start_ii = self.symbol.get_rolled_future().get_ticker_tables().index(self.src_tblname)
                dt_i = self.symbol.get_rolled_future().get_relative_roll_dates()[0]
                tbl = self.src_tblname
                for roll_ii in range(1, len(self.symbol.get_rolled_future().get_relative_roll_dates())):
                    dt_f = self.symbol.get_rolled_future().get_relative_roll_dates()[roll_ii]
#                     print "INSERT INTO " + self.tblname + " SELECT r.* FROM " + self.validSlice.get_tblname() + " l, " + tbl + " r WHERE l.ts=r.ts AND l.valid AND r.ts > DATE('" + dt_i + "') AND r.ts <= DATE('" + dt_f + "')"
                    self.dbh.execute("INSERT INTO " + self.tblname + " SELECT r.* FROM " + self.validSlice.get_tblname() + " l, " + tbl + " r WHERE l.ts=r.ts AND l.valid AND r.ts > DATE('" + dt_i + "') AND r.ts <= DATE('" + dt_f + "')")
                    dt_i = dt_f
                    if (roll_start_ii + roll_ii) < len(self.symbol.get_rolled_future().get_ticker_tables()):
                        tbl = self.symbol.get_rolled_future().get_ticker_tables()[roll_start_ii + roll_ii]
        return self.tblname
    
    def get_raw_signal(self, fld):
        if not self.signalHash.has_key(fld):
            self.signalHash[fld] = np.array(self.dbh.get_list("select "+fld+" from "+self.get_tblname()+" order by ts"))
        return self.signalHash[fld]
