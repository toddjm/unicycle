"""futures

"""

import mod_unicycle


class new():

    unicycle = None
    db = None
    instrument = None
    verbose = None

    exchange = None
    from_time = None
    to_time = None
    roll_date_sql = "DATE(IFNULL(adj_roll_date, adj_last_trade_date))"
    rel_contract_mos = None
    unadj_rel_contract_mos = None
    rel_roll_dates = None
    rel_contract_names = None
    ticker_tables = None
    ids_by_month = None

    def __init__(self, unicycle=None, instrument=None, db=None, verbose=False,
                 from_date=None, to_date=None):
        self.unicycle = unicycle
        self.verbose = verbose
        self.instrument = instrument
        self.dbh_def_tz = mod_unicycle.new().get_dbh()
        self.db = db
        self.from_date = from_date
        self.to_date = to_date

    def get_relative_roll_dates(self):
        if (self.rel_roll_dates == None):
            self.rel_roll_dates = self.dbh_def_tz.get_list(
              "SELECT DATE_FORMAT(%s, '%s') FROM futures_roll_dates"
              " WHERE id='%s' AND (ignore_period=0) ORDER BY"
              " adj_last_trade_date"
              % (self.roll_date_sql, "%Y%m%d", self.instrument))
            while (str(self.from_date).replace('-', '') >
                   self.rel_roll_dates[1]):
                self.rel_roll_dates.pop(0)
            while (str(self.to_date).replace('-', '') <
                   self.rel_roll_dates[-2]):
                self.rel_roll_dates.pop(-1)
        return self.rel_roll_dates

    def on_exit(self):
        self.dbh_def_tz.on_exit()

    def get_relative_contract_months(self):
        if (self.rel_contract_mos == None):
            self.rel_contract_mos = self.dbh_def_tz.get_list(
              "SELECT IFNULL(contract, DATE_FORMAT(last_trade_date, '%s'))"
              " FROM futures_roll_dates WHERE id='%s' AND (ignore_period=0)"
              " AND DATE('%s') <= %s ORDER BY adj_last_trade_date"
              % ("%Y%m", self.instrument, self.from_date, self.roll_date_sql))
        return self.rel_contract_mos

    def set_from_date(self, from_date):
        self.from_date = from_date
        self.clear()

    def set_instrument(self, instrument):
        self.instrument = instrument
        self.clear()

    def clear(self):
        self.rel_roll_dates = None
        self.rel_contract_mos = None
        self.unadj_rel_contract_mos = None

    def get_unadjusted_relative_contract_months(self):
        if (self.unadj_rel_contract_mos == None):
            self.unadj_rel_contract_mos = self.dbh_def_tz.get_list(
              "SELECT IFNULL(contract, DATE_FORMAT(last_trade_date, '%s'))"
              " FROM futures_roll_dates WHERE id='%s' AND (ignore_period=0)"
              " AND DATE('%s') <= DATE(last_trade_date) ORDER BY"
              " last_trade_date"
              % ("%Y%m", self.instrument, self.from_date))

        return self.unadj_rel_contract_mos

    def get_ids_by_month(self):
        if (self.ids_by_month == None):
            self.ids_by_month = self.unicycle.get_dbh().get_dict(
              "SELECT month, id FROM %s.futures_date_codes"
              % (self.unicycle.get("mysql", "default_db")))
        return self.ids_by_month

    def get_month_id(self, month):
        return self.get_ids_by_month()[month]

    def get_contract_month_name(self, date):
        return self.get_month_id(date[4:]) + date[3]

    def get_contract_name(self, date):
        return self.instrument + self.get_contract_month_name(date)

    def get_ticker_table_name(self, date):
        return self.db + "." + self.instrument + date + "_tks"

    def get_ticker_tables(self):
        if (self.ticker_tables == None):
            self.ticker_tables = list()
            for date in self.get_relative_contract_months():
                self.ticker_tables.append(self.get_ticker_table_name(date))
        return self.ticker_tables

    def set_attr(self):
        row = self.unicycle.get_dbh().get_row(
                "SELECT exchange, TIME_FORMAT(TIME(from_time), '%s'),"
                " TIME_FORMAT(TIME(to_time), '%s') FROM %s.futures WHERE"
                " id='%s'"
                % ('%H:%i:%s', '%H:%i:%s', self.unicycle.get("mysql",
                "default_db"), self.instrument))
        self.exchange = row[0]
        self.from_time = str(row[1])
        self.to_time = str(row[2])

    def get_exchange(self):
        if (self.exchange == None):
            self.set_attr()
        return self.exchange

    def get_from_time(self):
        if (self.from_time == None):
            self.set_attr()
        return self.from_time

    def get_to_time(self):
        if (self.to_time == None):
            self.set_attr()
        return self.to_time
