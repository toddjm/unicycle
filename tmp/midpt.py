#!/usr/bin/env python
import sys
from Signal_Model import zscale
import VectorSet
from pylab import plot, show, legend

symbols = sys.argv[2:]

vs = VectorSet.new(config_filename=sys.argv[1])
vs.addSymbolList(symbols)

for symbol in symbols:
    plot(zscale(vs[symbol].midpt), label=symbol)
legend()
show()
