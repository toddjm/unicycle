"""validts
"""
import mod_unicycle

class new():
    unicycle = None
    exchanges = None
    first_dts = None
    last_dts = None

    def __init__(self,
                 unicycle=None,
                 exchanges=None):
        self.unicycle = unicycle
        self.exchanges = exchanges

    def get_samples(self,
                    from_date,
                    to_date):
        samples = list()
        for exchange in self.get_exchanges():
            samples.append(self.get_unicycle().get_dbh().get_one(
              "SELECT COUNT(*) FROM %s WHERE ts >= '%s' AND ts < '%s'"
              % (
              self.get_valid_tbl(exchange),
              from_date,
              to_date)))
        samples.sort()
        return samples

    def get_exchanges(self):
        if (self.exchanges == None):
            self.exchanges = list()
        return self.exchanges

    def get_first_datetime(self,
                           index):
        if self.first_dts != None:
            return self.first_dts[index]
            
    def add_first_datetime(self,
                           dt=None,
                           exchange=None):
        if (self.first_dts == None):
            self.first_dts = list()
        if exchange != None:
            dt = mod_unicycle.get_dt_from_ts(
              self.get_unicycle().get_dbh().get_one(
              "SELECT ts FROM %s LIMIT 1"
              % (
              self.get_valid_tbl(
              exchange))))
        self.first_dts.append(dt)
        self.first_dts.sort()

    def get_last_datetime(self,
                          index):
        if self.last_dts != None:
            return self.last_dts[index]
            
    def add_last_datetime(self,
                          dt=None,
                          exchange=None):
        if (self.last_dts == None):
            self.last_dts = list()
        if exchange != None:
            dt = mod_unicycle.get_dt_from_ts(
              self.get_unicycle().get_dbh().get_one(
              "SELECT DATE_ADD(ts, INTERVAL %d SECOND) "
              "FROM %s ORDER BY ts DESC LIMIT 1"
              % (
              self.get_interval_seconds(),
              self.get_valid_tbl(
              exchange))))
        self.last_dts.append(dt)
        self.last_dts.sort()

    def get_interval_seconds(self):
        return int(
          self.get_unicycle().get(
          "interval",
           self.get_unicycle().get(
           "hires",
           "interval")))

    def get_valid_tbl(self,
                      exchange):
        return "%s.%s" % (self.get_unicycle().get("mysql", "default_db"),
          mod_unicycle.get_valid_table(
          exchange,
          self.get_unicycle().get(
          "hires",
          "interval")))
        
    def add_exchange(self,
                     exchange):
        if not exchange in self.get_exchanges():
            self.get_exchanges().append(exchange)
            self.add_first_datetime(exchange=exchange)
            self.add_last_datetime(exchange=exchange)

    def get_unicycle(self):
        return self.unicycle
