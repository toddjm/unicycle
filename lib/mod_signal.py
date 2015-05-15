"""signal

"""
from collections import deque
from mod_sql import get_random_name
import numpy as np


def rocind(data, idx):
    """Returns new array of `data` containing the `idx`-period
    rate of change indicator.

    """
    if len(data) == 0:
        return 0
    vals = [0.0] * idx
    for i in range(idx, len(data)):
        val = 100.0 * ((data[i] - data[i - idx]) / data[i - idx])
        vals.append(val)
    return np.array(vals)


def rsi(data, idx):
    """Returns new array of `s` containing the `idx`-period RSI"""
    if len(data) == 0:
        return 0
    avg_gain = 0.0
    avg_loss = 0.0
    rs = [0.0] * idx
    val = 0.0
    for i in range(1, idx + 1):
        val = data[i] - data[i - 1]
        if (val > 0.0):
            avg_gain += val
        elif (val < 0.0):
            avg_loss += abs(val)
    avg_gain /= float(idx)
    avg_loss /= float(idx)
    rs.append(avg_gain / avg_loss)
    for i in range(idx + 1, len(data)):
        val = data[i] - data[i - 1]
        if (val > 0.0):
            avg_gain = (avg_gain * (idx - 1) + val) / float(idx)
            avg_loss *= (idx - 1) / float(idx)
        elif (val < 0.0):
            avg_gain *= (idx - 1) / float(idx)
            avg_loss = (avg_loss * (idx - 1) + abs(val)) / float(idx)
        rs.append(avg_gain / avg_loss)
    for i in range(len(rs)):
        rs[i] = 100.0 - (100.0 / (1.0 + rs[i]))
    return np.array(rs)


def dpo(data, idx):
    """Returns a new array of `data` containing the `idx`-period
    detrended price oscillator (DPO).

    """
    if len(data) == 0:
        return 0
    p = (idx / 2) + 1
    smaval = sma(data, idx)
    vals = [data[idx - p] - data[0]]
    cnt = 1
    for i in range(idx, len(data)):
        vals.append(data[i - p] - smaval[cnt])
        cnt += 1
    return np.array(vals)


def sma(data, idx):
    """Returns new array of `data` containing the `idx`-period
    simple moving average of a sequence of numbers.

    """
    temp = deque([0.0] * idx)
    vals = list()
    dn = 0.0
    summ = 0.0
    for i in range(len(data)):
        temp.append(data[i])
        summ += data[i] - temp.popleft()
        dn = min(dn + 1, idx)
        vals.append(summ / dn)
    return np.array(vals)


def smooth(data, window_len, window):
    """Smooth"""
    if data.ndim != 1:
        return 0
    if data.size < window_len:
        return 0
    if window_len < 3:
        return 0
    if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        return 0
    s = np.r_[2 * data[0] - data[window_len:1:-1],
              data, 2 * data[-1] - data[-1:-window_len:-1]]
    if window == 'flat':
        w = np.ones(window_len, 'd')
    else:
        w = eval('np.' + window + '(window_len)')
    y = np.convolve(w / w.sum(), s, mode='same')
    return y[window_len - 1:-window_len + 1]


#def ema(x, alpha):
#    y = np.zeros(len(x))
#    for i in range(len(x)):
#        y[i+1] = y[i] + alpha * (x[i+1] - y[i])
#    return y


def ema_masked(signal, alpha, mask=None):
    """Smooth"""
    mask = (signal != signal) if mask == None else mask
    mask[0] = True
    val = 0.0
    vals = list()
    for i in range(len(signal)):
        val = signal[i] if mask[i] else (alpha * signal[i] +
                                         (1.0 - alpha) * val)
        vals.append(val)
    return np.array(vals)


def ema(signal, alpha):
    """Smooth"""
    if (type(alpha).__name__ == 'int'):
        alpha = float(2.0 / (alpha + 1.0))
    return ema_masked(signal, alpha)


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
    for i in range(len(s)):
        val = (s[i] + (i * val)) / (i + 1.0)
        vals.append(val)
    return np.array(vals)


def zscore(s):
    """Smooth"""
    return ((s - np.mean(s)) / np.std(s))


def zscale(s):
    """Returns new array of `s` with values on [-1,1]."""
    return (2.0 * (s - np.min(s)) / (np.max(s) - np.min(s))) - 1.0


def argfind(bool_signal):
    """Smooth"""
    return np.nonzero(bool_signal)[0]


def argcnt(bool_signal):
    """Smooth"""
    return len(argfind(bool_signal))


def argtake(signal, bool_signal):
    """Smooth"""
    return np.take(signal, argfind(bool_signal))


def lshift(signal, steps=1):
    """Return new array of `signal` shifted to the left by `steps` filling
       the end of the array with the last value in `signal` to preserve
       the original length

    """
    return np.append(np.delete(signal, slice(0, steps)),
                     np.repeat(signal[-1], steps))


def rshift(signal, steps=1, init=None):
    """Return new array of `signal` shifted to the right by `steps` filling
       the beginning of the array with the first value in `signal` (or the
       value of `init`) to preserve the original length

    """
    init = signal[0] if init == None else init
    return np.resize(np.insert(np.copy(signal), np.zeros(steps),
                     [init]), len(signal))


class ref():
    """Smooth"""
    signal = None
    signals = None

    def __getattr__(self, name):
        """Smooth"""
        return self.signal.__getattr__(name)

    def __getitem__(self, index):
        """Smooth"""
        return self.signals[index]

    def set_signal(self, signal, index=None):
        """Smooth"""
        if index == None:
            self.signal = signal
        else:
            if (self.signals == None):
                self.signals = {}
            self.signals[index] = signal


class new():
    """Smooth"""
    dbh = None
    validSlice = None
    src_tblname = None
    tblname = None
    signalHash = None

    def __init__(self, dbh, validSlice, src_tblname, symbol=None):
        """Smooth"""
        self.dbh = dbh
        self.src_tblname = src_tblname
        self.setValidSlice(validSlice)
        self.symbol = symbol

    def setValidSlice(self, validSlice):
        """Smooth"""
        self.validSlice = validSlice
        self.signalHash = {}

    def __getattr__(self, name):
        """Smooth"""
        return getattr(self, "_" + name)()

    def _open(self):
        """Smooth"""
        return self.get_raw_signal("open")

    def _ts(self):
        """Smooth"""
        return self.get_raw_signal("ts")

    def _close(self):
        """Smooth"""
        return self.get_raw_signal("close")

    def _high(self):
        """Smooth"""
        return self.get_raw_signal("high")

    def _low(self):
        """Smooth"""
        return self.get_raw_signal("low")

    def _volume(self):
        """Smooth"""
        return self.get_raw_signal("volume")

    def _wap(self):
        """Smooth"""
        return self.get_raw_signal("wap")

    def _vwap(self):
        """Smooth"""
        return self.wap

    def _count(self):
        """Smooth"""
        return self.get_raw_signal("count")

    def _midpt(self):
        """Smooth"""
        return (self.high + self.low) / 2.0

    def _range(self):
        """Smooth"""
        return (self.high - self.low)

    def _tprice(self):
        """Smooth"""
        return((self.high + self.low + self.close) / 3.0)

    def _mf(self):
        """Smooth"""
        return(self.tprice * self.volume)

    def _pvt(self):
        """Smooth"""
        if not self.signalHash.has_key('pvt'):
            summ = self.accdist_item(self.volume[0], self.high[0],
                                     self.low[0], self.close[0])
            vals = [summ]
            for i in range(1, len(self.open)):
                summ += self.volume[i] * ((self.close[i] - self.close[i - 1]) /
                                          self.close[i - 1])
                vals.append(summ)
            self.signalHash['pvt'] = np.array(vals)
        return self.signalHash['pvt']

    def _pdm(self):
        """Smooth"""
        if not self.signalHash.has_key('pdm'):
            vals = [0]
            for i in range(1, len(self.open)):
                um = self.high[i] - self.high[i - 1]
                dm = self.low[i - 1] - self.low[i]
                if (um > dm and um > 0.0):
                    vals.append(um)
                else:
                    vals.append(0.0)
            self.signalHash['pdm'] = np.array(vals)
        return self.signalHash['pdm']

    def _ndm(self):
        """Smooth"""
        if not self.signalHash.has_key('ndm'):
            vals = [0]
            for i in range(1, len(self.open)):
                um = self.high[i] - self.high[i - 1]
                dm = self.low[i - 1] - self.low[i]
                if (dm > um and dm > 0.0):
                    vals.append(dm)
                else:
                    vals.append(0.0)
            self.signalHash['ndm'] = np.array(vals)
        return self.signalHash['ndm']

    def _dmi(self):
        """Smooth"""
        if not self.signalHash.has_key('dmi'):
            vals = [0]
            for i in range(1, len(self.open)):
                val = (100.0 * abs(self.pdm[i] - self.ndm[i])) + 1.0
                val /= (self.pdm[i] + self.ndm[i]) + 1.0
                vals.append(val)
            self.signalHash['dmi'] = np.array(vals)
        return self.signalHash['dmi']

    def _tr(self):
        """Smooth"""
        if not self.signalHash.has_key('tr'):
            val = self.midpt[0]
            vals = [val]
            for i in range(1, len(self.open)):
                val = max(abs(self.high[i] - self.low[i]),
                          abs(self.high[i] - self.close[i - 1]),
                          abs(self.close[i - 1] - self.low[i]))
                vals.append(val)
            self.signalHash['tr'] = np.array(vals)
        return self.signalHash['tr']

    def accdist_item(self, volume, high, low, close):
        """Smooth"""
        return volume * self.clv_item(high, low, close)

    def _accdist(self):
        """Smooth"""
        if not self.signalHash.has_key('accdist'):
            vals = list()
            summ = 0.0
            for i in range(len(self.close)):
                summ += self.accdist_item(self.volume[i], self.high[i],
                                          self.low[i], self.close[i])
                vals.append(summ)
            self.signalHash['accdist'] = np.array(vals)
        return self.signalHash['accdist']

    def _obv(self):
        """Smooth"""
        if not self.signalHash.has_key('obv'):
            summ = self.accdist_item(self.volume[0], self.high[0],
                                     self.low[0], self.close[0])
            vals = [summ]
            for i in range(1, len(self.close)):
                summ += (self.volume[i] * cmp(self.close[i], self.close[i - 1]))
                vals.append(summ)
            self.signalHash['obv'] = np.array(vals)
        return self.signalHash['obv']

    def clv_item(self, high, low, close):
        """Smooth"""
        return 0 if high == low else ((close - low) - (high - close) /
                                      (high - low))

    def _clv(self):
        """Smooth"""
        if not self.signalHash.has_key('clv'):
            vals = list()
            for i in range(len(self.close)):
                vals.append(self.clv_item(self.high[i], self.low[i],
                                          self.close[i]))
            self.signalHash['clv'] = np.array(vals)
        return self.signalHash['clv']

    def get_tblname(self):
        """Smooth"""
        if (self.tblname == None):
#            self.tblname = self.dbh.get_random_name()
            self.tblname = get_random_name()
            self.dbh.execute('CREATE TEMPORARY TABLE ' + self.tblname +
                             ' LIKE ' + self.src_tblname)

            if not self.symbol.is_rolled_futures():
                self.dbh.execute('INSERT INTO ' +
                                 self.tblname +
                                 ' SELECT r.* from ' +
                                 self.validSlice.get_tblname() +
                                 ' l, ' + self.src_tblname +
                                 ' r where l.ts=r.ts and l.valid')
            else:
                roll_start_ii = self.symbol.get_rolled_future().get_ticker_tables().index(self.src_tblname)
                dt_i = self.symbol.get_rolled_future().get_relative_roll_dates()[0]
                tbl = self.src_tblname
                for roll_ii in range(1, len(self.symbol.get_rolled_future().get_relative_roll_dates())):
                    dt_f = self.symbol.get_rolled_future().get_relative_roll_dates()[roll_ii]
                    self.dbh.execute("INSERT INTO " +
                                     self.tblname +
                                     " SELECT r.* FROM " +
                                     self.validSlice.get_tblname() +
                                     " l, " + tbl +
                                     " r WHERE l.ts=r.ts AND l.valid AND r.ts > DATE('" +
                                     dt_i + "') AND r.ts <= DATE('" + dt_f + "')")
                    dt_i = dt_f
                    if (roll_start_ii + roll_ii) < len(self.symbol.get_rolled_future().get_ticker_tables()):
                        tbl = self.symbol.get_rolled_future().get_ticker_tables()[roll_start_ii + roll_ii]
        return self.tblname

    def get_raw_signal(self, fld):
        """Smooth"""
        if not self.signalHash.has_key(fld):
            self.signalHash[fld] = np.array(
                    self.dbh.get_list("select " + fld + " from " +
                                      self.get_tblname() + " order by ts"))
        return self.signalHash[fld]
