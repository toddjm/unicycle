"""tks

asset_class = (equities, futures, fx, fi, options)
data_source = ib
local_exchange = name of exchange as defined by data_source, e.g. ICE
                 is IPE for data_source ib.
local_instrument = name of instrument as defined by data_source, e.g. 6E is
                   EUR on local_exchange globex for data_source ib.
suffix = tks

db_tbl = <asset_class>_<data_source>.<local_instrument>_<suffix>
         e.g. fx_ib.EURUSD_tks

<raw_path> = <raw_dir>/<raw_filename> = <raw_dir>/<local_instrument>.<suffix>
<cfg_file> = <raw_dir>/<default_cfg>

input_dir = ${TICKS_HOME}/<asset_class>/<data_source>/<local_exchange>/<current_date>/<current_hour>/<date_str>
            e.g. /home/unicycleticks/fx/ib/idealpro/20110101/16/20101204

input_file = <input_dir>/<local_instrument>.<suffix>
            e.g. /home/unicycleticks/fx/ib/idealpro/20110101/16/20101204/EURUSD.tks

saved_input_file = <input_file>.<compressed_ext>
            e.g. /home/unicycleticks/fx/ib/idealpro/20110101/16/20101204/EURUSD.tks.bz2

link_dir = ${TICKS_HOME}/<asset_class>/<data_source>/<local_exchange>/<date_str>
            e.g. /home/unicycleticks/fx/ib/idealpro/20101204

link_file = <link_dir>/<local_instrument>.<suffix>
            e.g. /home/unicycleticks/fx/ib/idealpro/20101204/EURUSD.tks

saved_link_file = <link_file>.<compressed_ext>
                  e.g. /home/unicycleticks/fx/ib/idealpro/20101204/EURUSD.tks.bz2

ts_table = unicycle.<asset_class>_<data_source>_valid_ts
           e.g. unicycle.fx_ib_valid_ts



###<db_tbl> = <db>.<table> = <asset_class>_<interval>.<instrument>_<feed>
###<input_dir> = $(TICKS_HOME)/<exchange_dir>/<interval>/<current_date>/<current_hour>/<date_str>
###<input_file> = <input_dir>/<instrument>.<feed>
###<saved_input_file> = <input_file>.<compressed_ext>
###<link_dir> = $(TICKS_HOME)/<exchange_dir>/<interval>/<date_str>
###<link_file> = <link_dir>/<instrument>.<feed>
###<saved_link_file> = <link_file>.<compressed_ext>
###<ts_table> = unicycle.<exchange>_valid_<interval>
one-to-one relationships
exchange :: exchange_dir
exchange :: asset

"""
import mod_unicycle
import os
import re
import shutil
import warnings

def get_asset_class_from_db(db):
    return db.split('_')[0]

def get_asset_class_from_db_tbl(db_tbl):
    return get_asset_class_from_db(get_db_from_db_tbl(db_tbl))

def get_db_from_db_tbl(db_tbl):
    return db_tbl.split('.')[0]

def get_table_from_db_tbl(db_tbl):
    return db_tbl.split('.').pop()

class new(object):
    unicycle = None
    db_tbl = None
    db = None
    dbh = None
    table = None
    tables = None
    asset_class = None
    data_source = None
    local_exchange = None
    local_instrument = None
    cfg_file = None
    saved_input_file = None
    saved_link_file = None
    date_str = None
    sample_secs = None
    interval_secs = None
    interval_table = None
    suffix = None
    raw_path = None
    raw_dir = None
    raw_filename = None
    raw_table = None
    init_config_file = None
    input_compressed = None
    current_date = None
    current_hour = None
    ts_table = None
    verbose = False
    tmp_futures_valid_1day = None
    feed_index = None

    ohlc_flds = """
    `ts` TIMESTAMP NOT NULL DEFAULT 0,
    `open` FLOAT DEFAULT NULL,
    `high` FLOAT DEFAULT NULL,
    `low` FLOAT DEFAULT NULL,
    `close` FLOAT DEFAULT NULL,
    `volume` INT DEFAULT NULL,
    `wap` FLOAT DEFAULT NULL,
    `hasgaps` TINYINT(1) DEFAULT NULL,
    `count` INT DEFAULT NULL
    """
    ohlc_keys = "PRIMARY KEY (`ts`)"

    lores_flds = """
    `open_ts` TIMESTAMP NOT NULL DEFAULT 0,
    `close_ts` TIMESTAMP NOT NULL DEFAULT 0,
    `hi_count` INT DEFAULT NULL
    """
    lores_keys = """
    INDEX `open_ts_idx` (`open_ts`),
    INDEX `close_ts_idx` (`close_ts`),
    INDEX `hi_count_idx` (`hi_count`)
    """

    interval_table_flds = """
    `ts` TIMESTAMP NOT NULL DEFAULT 0,
    `period` INT DEFAULT NULL
    """
    interval_table_keys = "PRIMARY KEY (`ts`), INDEX `period_idx` (`period`)"

    mysql_table_type = "ENGINE=MYISAM"

    log_table_sql = """
    `fname` VARCHAR(128) NOT NULL DEFAULT '',
    `instrument` VARCHAR(16) NOT NULL,
    `exchange` VARCHAR(16) NOT NULL,
    `expiry` VARCHAR(6) NOT NULL,
    `currency` VARCHAR(3) NOT NULL,
    `include_expired` VARCHAR(1) NOT NULL,
    `bar_size` VARCHAR(12) NOT NULL,
    `duration` VARCHAR(12) NOT NULL,
    `use_rth` VARCHAR(1) NOT NULL,
    `end_time_date` VARCHAR(24) NOT NULL,
    `what_to_show` VARCHAR(12) NOT NULL,
    PRIMARY KEY (`fname`)
    """

    try_table_sql = """
    `instrument` VARCHAR(16) NOT NULL,
    `ts` TIMESTAMP NOT NULL DEFAULT 0,
    `tks_ts` TIMESTAMP NOT NULL DEFAULT 0,
    `tries` TINYINT(3) NOT NULL DEFAULT 0,
    INDEX `instrument_idx` (`instrument`),
    INDEX `tks_ts_idx` (`tks_ts`)
    """

    def __init__(self,
                 unicycle = None,
                 config_filename=None,
                 db=None,
                 db_tbl=None,
                 table=None,
                 asset_class=None,
                 data_source = None,
                 local_exchange=None,
                 local_instrument=None,
                 suffix="tks",
                 interval_secs="15",
                 raw_path=None,
                 raw_dir=None,
                 verbose=None
                 ):
        self.verbose = verbose
        self.unicycle = unicycle
        self.init_config_file = config_filename

        if (db_tbl != None):
            self.set_db_tbl(db_tbl=db_tbl)
        if (db != None):
            self.set_db(db=db)
        if (suffix != None):
            self.set_suffix(suffix=suffix)
        if (table != None):
            self.set_table(table=table)
        if (asset_class != None):
            self.set_asset_class(asset_class=asset_class)
        if (local_instrument != None):
            self.set_local_instrument(local_instrument=local_instrument)
        if (interval_secs != None):
            self.set_interval_secs(interval_secs=interval_secs)
        if (raw_dir != None):
            self.set_raw_dir(raw_dir=raw_dir)
        if (raw_path != None):
            self.set_raw_path(raw_path=raw_path)
        if (local_exchange != None):
            self.set_local_exchange(local_exchange=local_exchange)

    def make_collect_try_table(self):
        table = self.get("collect", "try_table")
        if not self.get_dbh().table_exists(table):
            self.get_dbh().execute("CREATE TABLE %s (%s) %s"
            % (table, self.try_table_sql, self.mysql_table_type))

    def set_unicycle(self, unicycle):
        self.unicycle = unicycle

    def get_unicycle(self):
        if (self.unicycle == None):
            self.unicycle = mod_unicycle.new(
              config_filename=self.init_config_file)
        return self.unicycle

    def get_dbh(self):
        if (self.dbh == None):
            self.get_db()
            self.dbh = self.get_unicycle().get_dbh()
        return self.dbh

    def get_db(self):
        if (self.db == None):
            self.db = "%s_%s" % (self.get_asset_class(), self.get_dat_source())
            if self.get_unicycle().get_dbh().db_exists(self.db):
                self.get_unicycle().get_dbh().select_db(self.db)
        return self.db

    def get_config(self):
        return self.get_unicycle().get_config()

    def get(self, section, name):
        return self.get_unicycle().get(section, name)

    def set_db_tbl(self, db_tbl):
        self.db_tbl = db_tbl
        self.set_db(db_tbl=db_tbl)
        self.set_table(db_tbl=db_tbl)

    def set_db(self, db_tbl=None, db=None):
        old_db = self.db
        clear = True
        if (db != None):
            self.db = db
        elif (db_tbl != None):
            self.db = get_db_from_db_tbl(db_tbl)
            clear = False
        else:
            self.db = None
            
        if (clear and (old_db != self.db)):
            self.clear_db_tbl()

        self.set_asset_class(db=self.db)
        self.set_interval(db=self.db)
        self.get_dbh().select_db(self.db)

    def set_table(self, db_tbl=None, table=None):
        old_table = self.table
        clear = True
        if (table != None):
            self.table = table
        elif (db_tbl != None):
            self.table = get_table_from_db_tbl(db_tbl)
            clear = False
        else:
            self.table = None

        if (clear and (old_table != self.table)):
            self.clear_db_tbl()
            self.clear_input_dir()

        self.set_local_instrument(table=self.table)
        self.set_suffix(table=self.table)

    def set_asset_class(self, db=None, asset_class=None):
        old_asset_class = self.asset_class
        clear = True
        if (asset_class != None):
            self.asset_class = asset_class
        elif (db != None):
            self.asset_class = get_asset_class_from_db(db)
            clear = False
        else:
            self.asset_class = None
            self.asset_class_class = None

        if (self.asset_class != None):
            self.asset_class_class = self.get("asset_class", self.asset_class)
            self.set_exchange(asset_class=asset_class, db=db)

        if (clear and (old_asset_class != self.asset_class)):
            self.clear_db()
            
    def set_interval(self, db=None, interval=None, interval_secs=None):
        old_interval = self.interval
        clear = True
        if (interval != None):
            self.interval = interval
        elif (interval_secs != None):
            self.interval = self.get_unicycle().get_key(
              "interval", interval_secs)
        elif (db != None):
            self.interval = db.split('_').pop()
            clear = False
        else:
            self.interval = None
        self.interval_secs = None

        if (clear and (old_interval != self.interval)):
            self.clear_db()
            self.clear_ts_table()

    def set_interval_secs(self, interval_secs=None):
        old_interval_secs = self.interval_secs

        if (interval_secs != None):
            self.interval_secs = int(interval_secs)
        else:
            self.interval = None
            self.interval_secs = None

        if (old_interval_secs != self.interval_secs):
            self.clear_db()

        if (self.get_interval_secs() != None):
            self.interval = self.get_unicycle().get_key("interval", self.get_interval_secs())

    def set_local_instrument(self, table=None, local_instrument=None, raw_filename=None):
        old_local_instrument = self.local_instrument
        clear = True
        if (local_instrument != None):
            self.local_instrument = local_instrument
        elif (table != None):
            flds = table.split('_')
            if (len(flds) > 1):
                flds.pop()
            self.local_instrument = '_'.join(flds)
            clear = False
        elif (raw_filename != None):
            self.local_instrument = raw_filename.split('.')[0]
        else:
            self.local_instrument = None

        if (clear and (old_local_instrument != self.local_instrument)):
            self.clear_table()

    def set_suffix(self, table=None, suffix=None, raw_filename=None):
        old_feed = self.feed
        clr_table = True
        clr_raw = True
        if (suffix != None):
            self.suffix = suffix
        elif (table != None):
            self.suffix = table.split('_').pop()
            clr_table = False
        elif (raw_filename != None):
            self.suffix = raw_filename.split('.').pop().split('_').pop(0)
            clr_raw = False
        else:
            self.suffix = None

        if (old_suffix != self.suffix):
            if (clr_table):
                self.clear_table()
            if (clr_raw):
                self.clear_raw_filename()

    def set_cfg_file(self, cfg_file=None, raw_dir=None):
        new_cfg = None
        if (cfg_file != None):
            new_cfg = cfg_file
        elif (raw_dir != None):
            new_cfg = self.find_cfg_from_path(raw_dir)
            if (new_cfg == None):
                raise StandardError("Fatal Error: cannot"
                  " find config file in %s." % (raw_dir))

        if ((new_cfg != None) and (new_cfg != self.cfg_file)):
            self.get_unicycle().set_local_cfg_file(new_cfg)

            if self.get_config().has_option("config", "data_source"):
                self.get_dbh().set_data_source(
                  self.get("config", "data_source"))

            if self.get_config().has_option("config", "asset_class"):
                self.get_dbh().set_asset_class(
                  self.get("config", "asset_class"))

            if self.get_config().has_option("config", "time_zone"):
                self.get_dbh().set_session_time_zone(
                  self.get("config", "time_zone"))

        self.cfg_file = new_cfg

    def find_cfg_from_path(self, path):
        dirs = path.split('/')
        while (len(dirs)):
            cfg = "%s/%s" % (
              '/'.join(dirs), self.get_unicycle().get_default_cfg_file())
            if os.path.exists(cfg):
                return cfg
            dirs.pop()
        return None

    def set_date_str(self, date_str=None):
        self.date_str = date_str
        self.clear_input_dir()

    def set_local_exchange(self,
                     local_exchange=None,
                     local_exchange_dir=None,
                     asset_class=None,
                     db=None):
        clear = True
        old_local_exchange = self.local_exchange
        if (local_exchange != None):
            self.local_exchange = local_exchange
        elif (local_exchange_dir != None):
            self.local_exchange = self.get_unicycle().get_key(
              "local_exchange_dir", local_exchange_dir)
        elif (asset_class != None):
            self.local_exchange = self.get_unicycle().get_key(
              "local_exchange_asset_class", asset_class)
        elif (db != None):
            self.local_exchange = self.get_unicycle().get_key(
              "local_exchange_asset_class", get_asset_class_from_db(db))
            clear = False
        else:
            self.local_exchange = None

        if (old_local_exchange != self.local_exchange):
            self.clear_input_dir()
            self.clear_link_dir()
            self.clear_ts_table()
            if clear:
                self.clear_asset_class()

    def set_local_exchange_dir(self, local_exchange_dir=None):
        self.local_exchange_dir = local_exchange_dir
        self.set_local_exchange(local_exchange_dir=local_exchange_dir)

    def set_raw_path(self, raw_path=None):
        self.raw_path = raw_path
        self.set_raw_dir(raw_path=self.raw_path)
        self.set_raw_filename(raw_path=self.raw_path)

    def set_raw_dir(self, raw_dir=None, raw_path=None):
        old_raw_dir = self.raw_dir
        clear = True
        if (raw_dir != None):
            self.raw_dir = raw_dir
        elif (raw_path != None):
            self.raw_dir = os.path.dirname(raw_path)
            clear = False
        else:
            self.raw_dir = None

        if (old_raw_dir != self.raw_dir):
            if (clear):
                self.clear_raw_path()
            self.set_cfg_file(raw_dir=self.raw_dir)

    def set_raw_filename(self, raw_filename=None, raw_path=None):
        old_raw_filename = self.raw_filename
        clear = True
        if (raw_filename != None):
            self.raw_filename = raw_filename
        elif (raw_path != None):
            self.raw_filename = os.path.basename(raw_path)
            clear = False
        else:
            self.raw_filename = None

        if (clear and (old_raw_filename != self.raw_filename)):
            self.clear_raw_path()

        self.set_local_instrument(raw_filename=self.raw_filename)
        self.set_suffix(raw_filename=self.raw_filename)

    def set_current_date(self):
        row = self.get_dbh().get_row(
          "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s'),"
          " DATE_FORMAT(NOW(), '%Y%m%d'), DATE_FORMAT(NOW(), '%H')")
        self.current_timestamp = row[0]
        self.current_date = row[1]
        self.current_hour = row[2]
        self.clear_input_dir()

    def set_current_hour(self):
        self.set_current_date()

    def is_try_cnt_OK(self, max_tries=None):
        return (self.get_collect_try_cnt() < 
          (int(self.get("collect", "max_tries"))
          if (max_tries == None) else max_tries))

    def is_collected(self):
        return (
                os.path.exists(self.get_saved_link_file()) or
                os.path.exists(self.get_link_file()) or
                  (
                  self.get_dbh().table_exists(self.get_table()) and
                  self.get_dbh().get_one(
                  "SELECT COUNT(*) FROM %s WHERE DATE(ts)='%s'"
                  % (self.get_table(), self.get_date_str()))
                  )
                )

    def get_collect_try_cnt(self):
        try_cnt = self.get_dbh().get_one("SELECT tries FROM collect WHERE"
          " local_instrument='%s' AND DATE(tks_ts)='%s'"
          % (self.get_local_instrument(), self.get_date_str()))
        if (try_cnt == None):
            self.get_dbh().execute("INSERT INTO collect (local_instrument, tks_ts)"
              " VALUES ('%s','%s %s')" % (self.get_local_instrument(),
              self.get_dbh().get_formatted_date_str(self.get_date_str()),
              self.get("lores", "1day_default_time")))
            return 0
        return try_cnt

    def increment_collect_try_cnt(self):
        self.get_dbh().execute(
          "UPDATE collect SET tries=tries+1, ts='%s' WHERE local_instrument='%s'"
          " AND DATE(tks_ts)='%s'"
          % (self.get_current_timestamp(), self.get_local_instrument(),
          self.get_date_str()))

    def get_raw_path(self):
        if (self.raw_path == None):
            self.raw_path = "%s/%s" % (self.get_raw_dir(), self.get_raw_filename())
        return self.raw_path

    def get_local_exchange(self):
        if (self.local_exchange == None):
            self.local_exchange = self.get("this", "local_exchange")
        return self.local_exchange

    def get_local_exchange_dir(self):
        if (self.local_exchange_dir == None):
            self.local_exchange_dir = self.get("local_exchange_dir", self.get_local_exchange())
        return self.local_exchange_dir
    
    def get_date_str(self):
        return self.date_str
    
    def get_current_timestamp(self):
        return self.current_timestamp
    
    def get_current_date(self):
        return self.current_date
    
    def get_current_hour(self):
        return self.current_hour
    
    def get_input_dir(self):
        if (self.input_dir == None):
            self.set_current_date()
            self.input_dir = "%s/%s/%s/%s/S%s/%s/%s" % (
              os.getenv("TICKS_HOME"),
              self.get_asset_class(),
              self.get_data_source(),
              self.get_local_exchange_dir(),
              self.get_current_date(),
              self.get_current_hour(),
              self.get_date_str())
            if not os.path.exists(self.input_dir):
                os.makedirs(self.input_dir)
                os.system(
                  "touch %s/%s/%s/%s/S%s_%s.rtks" % (
                  os.getenv("TICKS_HOME"),
                  self.get_asset_class(),
                  self.get_data_source(),
                  self.get_local_exchange_dir(),
                  self.get_current_date(),
                  self.get_current_hour()))
                shutil.copy(
                  "%s/%s/%s/%s/unicycle.cfg" % (
                  os.getenv("TICKS_HOME"),
                  self.get_asset_class(),
                  self.get_data_source(),
                  self.get_local_exchange_dir()),
                  self.input_dir)
        return self.input_dir

    def get_link_dir(self):
        if (self.link_dir == None):
            self.link_dir = "%s/%s/%s/%s/%s" % (
              os.getenv("TICKS_HOME"),
              self.get_exchange_dir(),
              "ib",
              self.get_interval(),
              self.get_date_str())
            if not os.path.exists(self.link_dir):
                os.makedirs(self.link_dir)
        return self.link_dir

    def get_input_file(self):
        if (self.input_file == None):
            self.input_file = "%s/%s.%s" % (
              self.get_input_dir(),
              self.get_local_instrument(),
              self.get_feed())
            if self.get_feed_index():
                self.input_file += "_%d" % (int(self.get_feed_index()))
        return self.input_file

    def get_link_file(self):
        if (self.link_file == None):
            self.link_file = "%s/%s.%s" % (
              self.get_link_dir(),
              self.get_local_instrument(),
              self.get_feed())
            if self.get_feed_index():
                self.link_file += "_%d" % (int(self.get_feed_index()))
        return self.link_file

    def get_feed_index(self):
        return self.feed_index
        
    def set_feed_index(self, index):
        self.saved_input_file = None
        self.input_file = None
        self.saved_link_file = None
        self.link_file = None
        self.feed_index = index

    def save_input_file(self):
        if not os.path.exists(self.get_input_file()):
            os.system("touch %s" % (self.get_input_file()))
        if (self.is_input_compressed()):
            os.system(
              "%s %s" % (
              self.get_unicycle().get("feed", "zip_command"),
              self.get_input_file()))
            os.system(
              "ln -s %s %s" % (
              self.get_saved_input_file(),
              self.get_saved_link_file()))
        else:
            os.system(
              "ln -s %s %s" % (
              self.get_input_file(),
              self.get_link_file()))

    def is_input_compressed(self):
        if (self.input_compressed == None):
            self.input_compressed = (
              self.get("feed", "use_compression") == "True")
        return self.input_compressed

    def get_compressed_ext(self):
        return self.get("feed", "zip_ext")

    def get_saved_input_file(self):
        if (self.saved_input_file == None):
            self.saved_input_file = self.get_input_file()
            if (self.is_input_compressed()):
                self.saved_input_file += ".%s" % (self.get_compressed_ext())
        return self.saved_input_file

    def get_saved_link_file(self):
        if (self.saved_link_file == None):
            self.saved_link_file = self.get_link_file()
            if (self.is_input_compressed()):
                self.saved_link_file += ".%s" % (self.get_compressed_ext())
        return self.saved_link_file

    def get_db_tbl(self):
        if (self.db_tbl == None):
            self.db_tbl = "%s.%s" % (self.get_db(), self.get_table())
        return self.db_tbl

    def is_valid_datestr(self, datestr):
        return self.get_dbh().get_one("SELECT COUNT(*) FROM %s WHERE"
                 " DATE(ts)='%s'" % (self.get_ts_table(), datestr))

    def get_ts_table(self):
        if (self.ts_table == None):
            self.ts_table = "%s.%s" % (
              self.get_unicycle().get("mysql", "default_db"),
              mod_unicycle.get_valid_table(exchange=self.get_exchange(),
                interval=self.get_interval()))
        return self.ts_table

    def get_tables(self, re_str=None):
        if (self.tables == None):
            raw_names = self.get_dbh().get_list("SHOW TABLES LIKE '%s_%s'" % ("%", self.get_feed() if (self.get_feed() != None) else "tks"))
#            raw_names = self.get_dbh().get_list("SHOW TABLES LIKE %%_%s" % (self.get_feed() if (self.get_feed() != None) else "tks"))
            if ((self.get_exchange() == "equities") or (re_str != None)):
                self.tables = list()
                for table in raw_names:
                    if (
                      re.search(
                      "^[A-Z][A-Z_]+_tks" if re_str == None
                      else re_str, table)):
                        self.tables.append(table)
            else:
                self.tables = raw_names
        return self.tables

    def get_summary_tablename(self):
        return "%s.%s_%s" % (
          self.get_unicycle().get_value("mysql", "default_db"),
          self.get_asset_class(),
          self.get_unicycle().get_value("collect", "summary_suffix"))

    def get_table(self):
        if (self.table == None):
            self.table = "%s_%s" % (
              self.get_local_instrument(),
              self.get_feed())
        return self.table

    def get_table_from_local_instrument(self, local_instrument):
        return "%s_%s" % (
          local_instrument,
          self.get_feed())

    def get_asset_class(self):
        if (self.asset_class == None):
            self.asset_class = self.get(
              "exchange_asset_class",
              self.get_exchange())
        return self.asset_class

    def get_asset_class_class(self):
        return self.asset_class_class

    def get_interval(self):
        return self.interval
            
    def get_interval_secs(self):
        if self.interval_secs == None:
            self.interval_secs = int(self.get_unicycle().get("interval", self.interval))
        return self.interval_secs
            
    def get_local_instrument(self):
        return self.local_instrument
            
    def get_commodity(self):
        return re.findall(
          "^([A-Z0-9]+)[0-9]{6}",
          self.get_local_instrument()).pop()
            
    def get_expiry(self):
        return re.findall(
          "^[A-Z0-9]+([0-9]{6})",
          self.get_local_instrument()).pop()
            
    def get_raw_local_instrument(self):
        return self.get_local_instrument().replace('_', ' ')

    def get_fx_currency_1(self):
        return self.get_local_instrument()[0:3]

    def get_fx_currency_2(self):
        return self.get_local_instrument()[3:]

    def get_feed(self):
        return self.feed
            
    def clear_cfg_file(self):
        self.cfg_file = None

    def clear_interval_secs(self):
        self.clear_interval()

    def clear_interval(self):
        self.interval = None
        self.interval_secs = None
        self.sample_secs = None
        self.clear_db()
        self.clear_ts_table()

    def clear_ts_table(self):
        self.ts_table = None

    def clear_asset_class_class(self):
        self.clear_asset_class()

    def clear_asset_class(self):
        self.asset_class = None
        self.asset_class_class = None
        self.clear_db()

    def clear_db(self):
        self.dbh = None
        self.db = None
        self.tables = None
        self.clear_db_tbl()

    def clear_table(self):
        self.table = None
        self.sample_secs = None
        self.clear_db_tbl()

    def clear_input_dir(self):
        self.input_dir = None
        self.clear_input_file()
        self.clear_link_dir()

    def clear_input_file(self):
        self.saved_input_file = None
        self.input_file = None

    def clear_link_dir(self):
        self.link_dir = None
        self.clear_link_file()

    def clear_link_file(self):
        self.saved_link_file = None
        self.link_file = None

    def clear_feed(self):
        self.feed = None
        self.clear_table()
        self.clear_raw_filename()

    def clear_db_tbl(self):
        self.db_tbl = None

    def clear_raw_dir(self):
        self.raw_dir = None
        self.clear_raw_path()

    def clear_raw_filename(self):
        self.raw_filename = None
        self.clear_raw_path()

    def clear_raw_path(self):
        self.raw_path = None

    def get_interval_table(self):
        if (self.interval_table == None):
            self.interval_table = "%s.%s" % (
              self.get_dbh().get_db_name(),
              self.get_dbh().get_random_name())
            self.get_dbh().execute(
              "CREATE TEMPORARY TABLE %s (%s, %s) %s" % (
              self.interval_table,
              self.interval_table_flds,
              self.interval_table_keys,
              self.mysql_table_type))
        return self.interval_table

    def clear_interval_table(self):
        if (self.interval_table != None):
            self.get_dbh().execute(
              "DELETE FROM %s" % (
              self.interval_table))

    def load_interval_table(self,
                            hi_db_tbl,
                            db=None):
        self.clear_interval_table()
        self.get_dbh().execute(
          "INSERT INTO %s " % (
          self.get_interval_table()) +
          "SELECT ts," +
          "(UNIX_TIMESTAMP(ts)-UNIX_TIMESTAMP(DATE_FORMAT(ts, '%s %s')))"
          " DIV %d " % (
          '%Y-%m-%d',
          self.get("this", "from_time"),
          self.get_interval_secs()) +
          "FROM %s" % (
          hi_db_tbl))

    def get_raw_table(self):
        if (self.raw_table == None):
            name = self.get_dbh().get_random_name()
            self.get_dbh().execute(
              "CREATE TEMPORARY TABLE %s.%s " % (
              self.get_db(),
              name) +
              "(%s, %s) %s" % (
              self.ohlc_flds,
              self.ohlc_keys,
              self.mysql_table_type))
            self.raw_table = "%s.%s" % (
              self.get_db(),
              name)
        return self.raw_table

    def clear_raw_table(self):
        if (self.raw_table != None):
            self.get_dbh().execute(
              "DELETE FROM %s" % (
              self.raw_table))

    def load_raw_table(self):
        self.clear_raw_table()
        self.get_dbh().execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES (ts,open,high,low,close,volume,wap,hasgaps,count)" % (self.get_raw_path(), self.get_raw_table()))

    def replace_into(self,
                     hi_db_tbl=None,
                     hi_interval_secs=None,
                     threshold=None,
                     default_time=None):
        dbh = self.get_dbh()

        if not dbh.db_exists(self.get_db()):
            dbh.execute(
              "CREATE DATABASE %s" % (
              self.get_db()))

        dbh.select_db(self.get_db())

        if (hi_db_tbl != None):

            if not dbh.table_exists(self.get_table()):
                dbh.execute(
                  "CREATE TABLE %s (%s, %s, %s, %s) %s" % (
                  self.get_table(),
                  self.ohlc_flds,
                  self.lores_flds,
                  self.ohlc_keys,
                  self.lores_keys,
                  self.mysql_table_type))

            self.load_interval_table(hi_db_tbl)

            if not self.get_interval() == "1day":
                ts_str = "FROM_UNIXTIME(UNIX_TIMESTAMP(MIN(a.ts))-MOD(UNIX_TIMESTAMP(MIN(a.ts)), %d))" % (self.get_interval_secs())
                from_str = "%s a WHERE a.hi_count < %f" % (self.get_table(),
                (self.get_interval_secs() / int(hi_interval_secs)) *
                float(threshold))
            else:
                ts_str = "TIMESTAMP(CONCAT(DATE_FORMAT(a.ts, '%s'), ' %s'))" % ('%Y-%m-%d', default_time)
                from_str = "%s a, %s v WHERE (a.ts = v.ts) AND (a.hi_count < (v.hi_count * %f))" % (self.get_table(),
                self.get_ts_table() if not (self.get_unicycle().get("exchange_asset_class", self.get_exchange()) == "futures") else self.get_tmp_futures_valid_1day(self.get_futures_1day_sample_secs() / int(hi_interval_secs)), float(threshold))

            dbh.execute(
                "REPLACE INTO %s " % self.get_table() +
                "SELECT %s," % ts_str +
                "NULL, MAX(high), MIN(low), NULL, SUM(volume), "
                "SUM(volume * wap) / SUM(volume), SUM(hasgaps) "
                "<> 0, SUM(count), MIN(a.ts), MAX(a.ts), COUNT(*) "
                "FROM %s a, %s b " % (
                hi_db_tbl,
                self.get_interval_table()) +
                "WHERE a.ts=b.ts " +
                "GROUP BY DATE(a.ts), period")

            # set open and close from hires table
            dbh.execute(
              "UPDATE %s x, %s a SET x.open=a.open "
              "WHERE x.open_ts=a.ts" % (
              self.get_table(),
              hi_db_tbl))
            dbh.execute(
              "UPDATE %s x, %s a SET x.close=a.close "
              "WHERE x.close_ts=a.ts"
              % (
              self.get_table(),
              hi_db_tbl))

            # delete lores records generated by too few hires records
            if self.verbose:
                total_cnt = dbh.get_one(
                  "SELECT COUNT(*) FROM %s"
                  % (
                  self.get_table()))
                for rec in dbh.get_all(
                  "SELECT a.ts, a.hi_count FROM %s"
                  % (
                  from_str)):
                    print "%s: %s = %d (%ssec sample cnt)" % (
                    self.get_local_instrument(),
                    rec[0],
                    rec[1],
                    hi_interval_secs)

            dbh.execute(
              "DELETE a.* FROM %s"
              % (
              from_str))

            if self.verbose:
                cnt = int(dbh.get_one(
                  "SELECT ROW_COUNT()"))
                if cnt:
                    print "%s: Deleted: %d of %d" % (
                    self.get_local_instrument(),
                    cnt,
                    total_cnt)
        else:
            if not dbh.table_exists(self.get_table()):
                dbh.execute(
                  "CREATE TABLE %s (%s, %s) %s" % (
                  self.get_table(),
                  self.ohlc_flds,
                  self.ohlc_keys,
                  self.mysql_table_type))

            self.load_raw_table()
            dt = dbh.get_one(
              "SELECT DATE_FORMAT(ts, '%s') FROM %s LIMIT 1" % (
              '%Y%m%d',
              self.get_raw_table()))
            dbh.execute(
              "REPLACE INTO %s SELECT * FROM %s" % (
              self.get_table(),
              self.get_raw_table()))
            dbh.execute(
              "REPLACE INTO %s SELECT * FROM %s WHERE DATE(ts)='%s'" % (
              self.get_raw_table(),
              self.get_table(),
              dt))

        dbh.execute(
          "OPTIMIZE TABLE %s" % (
          self.get_table()))

    def get_futures_1day_sample_secs(self):
        return int(
          self.get_dbh().get_one(
            "SELECT (UNIX_TIMESTAMP(to_time) "
            "- UNIX_TIMESTAMP(from_time)) "
            "FROM %s.futures WHERE id='%s'" % (
            self.get_unicycle().get("mysql", "default_db"),
            self.get_commodity())))

    def get_tmp_futures_valid_1day(self,
                                   samples=None):
        self.tmp_futures_valid_1day = "futures_valid"
        self.get_dbh().drop_tmp_table(self.tmp_futures_valid_1day)
        self.get_dbh().execute(
          "CREATE TEMPORARY TABLE %s LIKE %s" % (
          self.tmp_futures_valid_1day,
          self.get_ts_table()))
        self.get_dbh().execute(
          "INSERT INTO %s SELECT ts, IF(%d < hi_count, %d, "
          "hi_count) FROM %s" % (
          self.tmp_futures_valid_1day,
          samples,
          samples,
          self.get_ts_table()))
        return self.tmp_futures_valid_1day

    def get_sample_secs(self):
        if self.sample_secs == None:
            if not self.interval == None:
                if self.interval != "1day":
                    self.sample_secs = int(
                      self.get_unicycle().get(
                      "interval", self.interval))
                elif self.get_unicycle().get(
                       "asset_class",
                       self.get_exchange()) == "futures":
                    self.sample_secs = self.get_futures_1day_sample_secs()
                else:
                    self.sample_secs = int(
                      self.get_unicycle().get_dbh().get_one(
                      "SELECT hi_count FROM %s ORDER BY ts LIMIT 1" % (
                      self.get_ts_table()))) * int(
                      self.get_unicycle().get(
                      "interval",
                      self.get_unicycle().get(
                        "hires",
                        "interval")))
        return self.sample_secs
            
