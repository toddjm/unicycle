"""symbol
"""

import mod_futures
import re
import mod_tks

class new(mod_tks.new):

    db_type = None

    where = None
    where_str = None
    month_str = None

    from_date = None
    to_date = None

    tbl_list = None
    tbl_contract_month = None
    tbl_rel_contract_month = None
    contract_month_list = None
    contract_name_list = None
    rolled_futures = None
    eq_equities = None
    eq_fx = None
    eq_futures = None
    eq_rolled_futures = None
    signals = None

    def __init__(self, unicycle=None, asset=None, table=None, interval=None,
                 where=None, month_str=None, from_date=None, to_date=None):
        super(new, self).__init__(unicycle=unicycle, asset=asset,
              table=table, interval=interval)
        self.where = where
        self.month_str = month_str
        self.from_date = from_date
        self.to_date = to_date

    def get_tbl_list(self):
        if (self.tbl_list == None):
            self.tbl_list = list()
            if (self.is_equities() or self.is_fx() or self.is_futures()):
                self.tbl_list.append(self.get_db_tbl())
            elif self.is_rolled_futures():
                for ii in self.get_contract_month_list():
                    self.tbl_list.append(
                      self.get_rolled_futures().get_ticker_tables()[ii])
        return self.tbl_list

    def get_db_tbl(self):
        if (self.db_tbl == None):
            if (self.is_equities() or
                self.is_fx() or
                self.is_rolled_futures() or
                self.is_tbl_contract_month()):
                self.db_tbl = super(new, self).get_db_tbl()
            elif self.is_tbl_rel_contract_month():
                self.set_db_tbl("%s_tks" % (self.get_tbl_contract_month()))
        return self.db_tbl

    def is_equities(self):
        if (self.eq_equities == None):
            self.eq_equities = (self.get_db_type() == "equities")
        return self.eq_equities

    def is_fx(self):
        if (self.eq_fx == None):
            self.eq_fx = (self.get_db_type() == "fx")
        return self.eq_fx

    def is_futures(self):
        if (self.eq_futures == None):
            self.eq_futures = (self.get_db_type() ==
            "futures") and (self.month_str == None)
        return self.eq_futures

    def is_rolled_futures(self):
        if (self.eq_rolled_futures == None):
            self.eq_rolled_futures = (self.get_db_type() ==
            "futures") and (self.month_str != None)
        return self.eq_rolled_futures

    def is_tbl_contract_month(self):
        if (self.tbl_contract_month == None):
            self.tbl_contract_month = re.search(
              "\d{6}_tks$", self.get_table()) != None
        return self.tbl_contract_month

    def is_tbl_rel_contract_month(self):
        if (self.tbl_rel_contract_month == None):
            self.tbl_rel_contract_month = re.search(
              "[A-Z]\d$", self.get_table()) != None
        return self.tbl_rel_contract_month

    def get_rolled_futures(self):
        if (self.rolled_futures == None):
            self.rolled_futures = mod_futures.new(unicycle=self.get_unicycle(),
            instrument=self.get_instrument(), db=self.get_db(),
            from_date=self.from_date, to_date=self.to_date)
        return self.rolled_futures

    def __getitem__(self, index):
        return self.signals[index]

    def set_signal(self, signal, index):
        if (self.signals == None):
            self.signals = {}
        self.signals[self.get_contract_month_list()[index]] = signal

    def get_where_str(self):
        if (self.where_str == None):
            if (self.is_equities() or self.is_futures() or self.is_fx()):
                self.where_str = self.where
            elif self.is_rolled_futures():
                terms = list()
                terms.append(self.where)
                terms.append("TIME(r.ts) >= '" +
                  self.get_rolled_futures().get_from_time() + "'")
                terms.append("TIME(r.ts) < '" +
                  self.get_rolled_futures().get_to_time() + "'")
                self.where_str = ' AND '.join(terms)
        return self.where_str

    def get_db_type(self):
        if (self.db_type == None):
            self.db_type = self.get_config().get("config", self.get_asset())
        return self.db_type

    def get_tbl_rel_contract_month(self):
        return "%s%s%s" % (self.get_instrument()[0:-6],
          self.get_id_by_month(self.get_instrument()[-2:]),
          self.get_instrument()[-3])

    def get_tbl_contract_month(self):
        return "%s.%s%s%s" % (self.get_db(), self.get_table()[0:-2],
          str(int(self.get_config().get("config", "futures_decade")) +
          int(self.get_table()[-1])),
          self.get_month_by_id(self.get_table()[-2]))

    def get_month_by_id(self, id):
        return self.get_dbh().get_one(
          "SELECT month FROM %s.futures_date_codes WHERE id='%s'"
          % (self.get_config().get("mysql", "default_db"), id))

    def get_id_by_month(self, month):
        return self.get_dbh().get_one(
          "SELECT id FROM %s.futures_date_codes WHERE month='%s'"
          % (self.get_config().get("mysql", "default_db"), month))

    def get_contract_month_list(self):
        if (self.contract_month_list == None):
            self.contract_month_list = list()
            for ii_str in self.month_str.split(','):
                self.contract_month_list.append(int(ii_str))
        return self.contract_month_list
