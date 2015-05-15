#!/usr/bin/env python
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import numpy
import sys
import VectorSet
import Signal_Model
from Signal_Model import lshift, rshift, argtake
import pylab

class MyModel(Signal_Model.new):
    pass

vs = VectorSet.new(config_filename=sys.argv[1],
			signal_class=MyModel,
			verbose=False)

# this assigns the ticker symbol as a global variable in this scope
for key, value in vs.get_signals().iteritems():
    SPY = value
    globals()[key] = value

# model: SPY.midpt[t] - SPY.midpt[t-1]
# buy threshold is 0.165 from min(top 10% of lift)
# sell threshold is -0.175 from max(top 10% of lift)

# buy_threshold = 0.235
# sell_threshold = -0.255

buy_threshold = 0.02
sell_threshold = -0.02
max_position = 1000
order_size = 1000

position = 0
value = 0.0
balance = 0.0
profit_loss = 0.0
total_profit_loss = 0.0

period = len(SPY.midpt)
shares = 0

ii = 1
old_date_str = vs.get_ts()[ii].date()
while (ii <= period):
    
    if ((ii == period) or (vs.get_ts()[ii].date() != old_date_str)):
        jj = ii - 1
        if (position > 0):
            balance += (position * SPY.high[jj])
            print "%s: END SELL %d at $%3.2f pos = 0, p&l = $%3.2f" % (vs.get_ts()[jj], position, SPY.high[jj], balance)
        elif (position < 0):
            balance -= (abs(position) * SPY.low[jj])
            print "%s: END BUY %d at $%3.2f pos = 0, p&l = $%3.2f" % (vs.get_ts()[jj], abs(position), SPY.low[jj], balance)
            
        shares += abs(position)
        total_profit_loss += balance
        print("Total P&L: $%3.2f" % (total_profit_loss))
        print
        position = 0
        balance = 0.0
 
    if (ii != period):

        score = SPY.midpt[ii] - SPY.midpt[ii-1]
        # print "%s: [%3.3f]" % (vs.get_ts()[ii], score)
        # buy_price = SPY.high[ii]
        # sell_price = SPY.low[ii]
        buy_price = SPY.midpt[ii]
        sell_price = SPY.midpt[ii]
        old_date_str = vs.get_ts()[ii].date()

        if (score > buy_threshold and position < max_position):
            shares += order_size
            position += order_size
            balance -= (order_size * buy_price)
            profit_loss = balance + (sell_price * position) if position > 0 else balance + (buy_price * position)
            print("%s: BUY [%3.2f] %d at $%3.2f bal = $%3.2f pos = %d, p&l = $%3.2f" % (vs.get_ts()[ii], score, order_size, buy_price, balance, position, profit_loss))
        elif (score < sell_threshold and position > (-1 * max_position)):
            shares += order_size
            position -= order_size
            balance += (order_size * sell_price)
            profit_loss = balance + (sell_price * position) if position > 0 else balance + (buy_price * position)
            print("%s: SELL [%3.2f] %d at $%3.2f bal = $%3.2f pos = %d, p&l = $%3.2f" % (vs.get_ts()[ii], score, order_size, sell_price, balance, position, profit_loss))

    ii += 1

print("Shares = %d" % (shares))
print("Total P&L Minus Commissions: $%3.2f" % (total_profit_loss - (0.005 * shares)))

pylab.plot(vs.get_ts(), SPY.midpt)
# pylab.legend()
pylab.show()
