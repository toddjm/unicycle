"""vectorset

"""

import numpy as np
import mod_signal
import mod_symbol
import mod_unicycle
import mod_vectorslice


class new():

    dbh = None

    signal_class = None
    valid_ts_table = None

    from_date = None
    to_date = None
    from_time = None
    to_time = None
    interval = None

    signal_cfg = None
    verbose = None

    validSlice = None
    default_exchange = None

    raw_ts = None
    ts = None
    andSlice = None

    valid_mask = None
    raw_day_mask = None
    day_mask = None

    symbolList = None

    slices = {}
    signal = {}
    signal_list = None

    signals = None
    signal_refs = None

    def __init__(self,
                 unicycle=None,
                 config_filename=None,
                 signal_class=mod_signal.new,
                 from_date=None,
                 to_date=None,
                 from_time=None,
                 to_time=None,
                 interval=None,
                 verbose=False):

        self.verbose = verbose
        self.unicycle = unicycle if not unicycle == None else mod_unicycle.new(config_filename)
        self.dbh = self.unicycle.get_dbh()
        self.signal_class = signal_class

        self.from_date = from_date
        self.to_date = to_date
        self.from_time = from_time
        self.to_time = to_time
        self.interval = interval

        self.symbolList = list()
        if self.unicycle.get_list("this", "signals") != None:
            for signalStr in self.unicycle.get_list("this", "signals"):
                atts = signalStr.split('\t')
                self.symbolList.append(
                  mod_symbol.new(
                  unicycle=self.unicycle,
                  asset=atts[0],
                  table=atts[1],
                  where=atts[2],
                  month_str=None if (len(atts) < 4) else atts[3],
                  interval=self.get_interval(),
                  from_date=self.get_from_date(),
                  to_date=self.get_to_date()))

    def get_signals(self):
        if (self.signal_refs == None):
            for symb in self.symbolList:
                for tbl in symb.get_tbl_list():
                    self.andValidSlice(tbl,
                                       where=symb.get_where_str(),
                                       symbol=symb)
            if self.verbose:
                self.getValidSlice().print_table()
            self.signals = {}
            self.signal_refs = {}
            self.updateSignals()
        return self.signal_refs

    def addSignals(self,
                   signals):
        if len(signals):
            self.default_exchange = signals[0].get_exchange()
            for signal in signals:
                self.symbolList.append(
                  mod_symbol.new(
                  unicycle=self.unicycle,
                  asset=self.unicycle.get(
                  "exchange_asset",
                  signal.get_exchange()),
                  table=signal.get_table(),
                  where=signal.get_where(),
                  month_str=signal.get_month_str(),
                  interval=self.get_interval(),
                  from_date=self.get_from_date(),
                  to_date=self.get_to_date()))

    def addSymbolList(self,
                      symbolList):
        self.get_signals()
        for symbol in symbolList:
            table = self.getTableNameFromSymbol(symbol)
            self.symbolList.append([table, "1=1"])
            self.andValidSlice(table, where="1=1")
        self.updateSignals()

    def getValidSlice(self):
        if (self.validSlice == None):
            self.validSlice = mod_vectorslice.new(
              dbh=self.dbh,
              left_table=self.get_valid_ts_table(),
              where=self.getValidWhereSQL(),
              verbose=self.verbose)
        return self.validSlice

    def getAndValidSlice(self,
                         table,
                         left_table=None,
                         where="1=1",
                         symbol=None):
        if (left_table == None):
            left_table = self.getValidSlice().get_tblname()
#        left_table = self.getValidSlice().get_tblname() if (left_table == None) else left_table
        return mod_vectorslice.new(dbh=self.dbh, left_table=left_table, table=table, where=where, verbose=self.verbose, symbol=symbol)

    def andValidSlice(self, table, where="1=1", symbol=None):
        curr_table = self.getValidSlice().get_tblname()
        self.validSlice = self.getAndValidSlice(
          left_table=curr_table,
          table=table,
          where=where,
          symbol=symbol)
        self.dbh.execute("DROP TABLE %s" % (curr_table))

    def get_valid_ts_table(self):
        if (self.valid_ts_table == None):
            self.valid_ts_table = "%s.%s" % (
              self.unicycle.get(
              "mysql",
              "default_db"),
              mod_unicycle.get_valid_table(
              exchange=self.get_default_exchange(),
              interval=self.get_interval()))
        return self.valid_ts_table

    def get_default_exchange(self):
        if self.default_exchange == None:
            self.default_exchange = self.unicycle.get("this", "default_asset")
        return self.default_exchange

    def updateSignals(self):
        self.signal_list = list()
        for symb in self.symbolList:
            index = 0
            for tbl in symb.get_tbl_list():
                alias = self.getAliasFromTablename(tbl)
                if not self.signals.has_key(alias):
                    self.signals[alias] = self.signal_class(
                      self.dbh,
                      self.validSlice,
                      tbl,
                      symbol=symb)
                else:
                    self.signals[alias].setValidSlice(self.validSlice)
                self.signal_list.append(self.signals[alias])

                # CL201003, CL
                self.get_signal_ref(alias).set_signal(self.signals[alias])

                if symb.is_rolled_futures():
                    # CL[0]
                    self.get_signal_ref(
                      symb.get_instrument()).set_signal(
                      self.signals[alias],
                      index)

                index += 1

            if symb.is_futures():
                # CLH1
                self.get_signal_ref(
                  symb.get_tbl_rel_contract_month()).set_signal(
                  self.signals[alias])

    def get_signal_ref(self,
                       key):
        if not self.signal_refs.has_key(key):
            self.signal_refs[key] = mod_signal.ref()
        return self.signal_refs[key]

    def getTableNameFromSymbol(self,
                               symbol):
        return self.unicycle.get("this", "default_db") + "." + symbol + "_tks"

    def addFuture(self,
                  symbol,
                  where="1=1"):
        pass

    def addSymbol(self,
                  symbol,
                  where="1=1"):
        self.addTable(self.getTableNameFromSymbol(symbol), where=where)

    def addTable(self,
                 table,
                 where="1=1"):
        self.get_signals()
        self.symbolList.append([table, where])
        self.andValidSlice(table, where=where)
        self.updateSignals()

    def getAliasFromTablename(self,
                              tblname):
        return tblname.split('.').pop().replace('_tks', '')

    def get_from_date(self):
        if self.from_date == None:
            self.from_date = self.unicycle.get("this", "from_date")
        return self.from_date

    def get_to_date(self):
        if self.to_date == None:
            self.to_date = self.unicycle.get("this", "to_date")
        return self.to_date

    def get_from_time(self):
        if self.from_time == None:
            self.from_time = self.unicycle.get("this", "from_time")
        return self.from_time

    def get_to_time(self):
        if self.to_time == None:
            self.to_time = self.unicycle.get("this", "to_time")
        return self.to_time

    def get_interval(self):
        if self.interval == None:
            self.interval = self.unicycle.get("this", "interval")
        return self.interval

    def getValidWhereSQL(self):
        terms = list()
        terms.append(
          "ts >= '%s'"
          % (
          self.get_from_date()))
        terms.append(
          "ts < '%s'"
          % (
          self.get_to_date()))
        terms.append(
          "TIME(ts) >= '%s'"
          % (
          self.get_from_time()))
        terms.append(
          "TIME(ts) < '%s'"
          % (
          self.get_to_time()))
        return ' AND '.join(terms)

    def __setitem__(self,
                    key,
                    obj):
        self.signal[key] = np.array(obj)

    def __getitem__(self,
                    obj):
        if mod_unicycle.is_integer(obj):
            return self.signal_list[obj]
        return self.get_signal(obj)

    def get_signal(self,
                   obj):
        return self.signals[obj]

    def get_valid_mask(self):
        if self.valid_mask == None:
            self.valid_mask = (np.array(
              self.dbh.get_list(
              "SELECT valid FROM %s ORDER BY ts"
              % (
              self.validSlice.get_tblname()))) == 1)
        return self.valid_mask

    def get_raw_day_mask(self):
        if self.raw_day_mask == None:
            self.raw_day_mask = (np.array(
              self.dbh.get_list(
              "SELECT TIME(ts)= '%s' FROM %s ORDER BY ts"
              % (
              self.get_from_time(),
              self.validSlice.get_tblname()))) == 1)
        return self.raw_day_mask

    def get_shifted_day_mask(self,
                             lsteps=0,
                             rsteps=0):
        indices = list()
        for index in mod_signal.argfind(self.get_day_mask()):
            indices += range(max(index - lsteps, 0), index)
            indices += range(index + 1, min(index + rsteps,
                       len(self.get_day_mask())))
        mask = np.copy(self.get_day_mask())
        for index in indices:
            mask[index] = True
        return mask

    def get_day_mask(self):
        if self.day_mask == None:
            self.day_mask = np.copy(self.get_raw_day_mask())
            if not np.all((self.get_valid_mask() & self.day_mask) ==
               self.day_mask):
                search = False
                for index in range(len(self.day_mask)):
                    if search and self.get_valid_mask()[index]:
                        self.day_mask[index] = True
                        search = False
                    elif (self.day_mask[index] and not
                          self.get_valid_mask()[index]):
                        search = True
            self.day_mask = mod_signal.argtake(
                            self.day_mask,
                            self.get_valid_mask() == True)
        return self.day_mask

    def ema(self,
            signal,
            alpha):
        return mod_signal.ema_masked(signal, alpha, self.get_day_mask())

    def keys(self):
        return self.signal.keys()

    def has_key(self,
                key):
        return self.signal.has_key(key)

    def get_vector_slice(self,
                         alias):
        return self.slices[alias]

    def get_raw_ts(self):
        if self.raw_ts == None:
            self.raw_ts = np.array(
                          self.dbh.get_list(
                          "select ts from " +
                          self.validSlice.get_tblname() +
                          " order by ts"))
        return self.raw_ts

    def get_ts(self):
        if self.ts == None:
            if not self.validSlice == None:
                self.ts = np.array(
                          self.dbh.get_list(
                          "select ts from " +
                          self.validSlice.get_tblname() +
                          " where valid = 1 order by ts"))
        return self.ts
