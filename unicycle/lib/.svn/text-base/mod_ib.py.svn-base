""" ib

"""

import datetime
import os
import re
import mod_sql
import subprocess
import time
import mod_tks
import mod_unicycle
import warnings


class asset(object):

    tks = None
    date = None
    pexch = None
    mult = None

    def __init__(self, tks, pexch, mult, date=None):
        self.tks = tks
        self.pexch = pexch
        self.mult = mult
        self.set_date(date)

    def get_constraint(self, **kwargs):
        return "(DATE(ts) >= '%s')" % (self.tks.get_dbh().get_date_add(None, "-1 YEAR"))

    def set_date(self, date):
        self.date = date

    def get_symbol(self):
        return self.tks.get_raw_instrument()

    def get_exchange_type(self):
        return self.tks.get("exchange_type", self.tks.get_exchange())

    def get_from_time(self):
        return self.tks.get("this", "from_time")

    def get_to_time(self):
        return self.tks.get("this", "to_time")

    def get_unix_from_time(self):
        return self.tks.get_dbh().get_unix_timestamp(self.tks.get("this", "from_time"))

    def get_unix_to_time(self):
        return self.tks.get_dbh().get_unix_timestamp(self.tks.get("this", "to_time"))

    def get_samples(self):
        total = ((self.get_unix_to_time() - self.get_unix_from_time())
                + int(self.tks.get_interval_secs())) / self.tks.get_interval_secs()
        out = list()
        while total > 0:
            sub = min(total, int(self.tks.get("IB", "max_collect_samples")))
            out.append(sub)
            total -= sub
        return out
        
    def get_end_str(self, sample_cnt):
        return "%s %s %s" % (self.date,
          self.tks.get_dbh().get_time_add_seconds(
          self.get_from_time(), sample_cnt * self.tks.get_interval_secs()),
          mod_sql.time_zone_map[self.tks.get("this", "time_zone")][0:3])

    def get_duration_str(self, sample_cnt):
        return "%s s" % (sample_cnt * int(self.tks.get_interval_secs()))

    def get_primary_exchange_str(self):
        if not self.pexch is None:
            if self.pexch.has_key(self.tks.get_instrument()):
                return "-primary_exchange %s" % (self.pexch[self.tks.get_instrument()])
        return ""

    def get_multiplier_str(self):
        if not self.mult is None:
            if self.mult.has_key(self.tks.get_commodity()):
                return "-multiplier %s" % (self.mult[self.tks.get_commodity()])
        return ""

    def get_collect_cmds(self):
        out = list()
        running_sample_cnt = 0
        for ii, sample_cnt in enumerate(self.get_samples()):
            self.tks.set_feed_index(ii)
            running_sample_cnt += sample_cnt
            out.append(
              "java Collect.Asset -asset %s -s \"%s\" -end \"%s\""
              " -durationStr \"%s\" -o %s -barSizeSetting \"%d secs\""
              " %s %s" % (
                self.get_exchange_type(),
                self.get_symbol(),
                self.get_end_str(running_sample_cnt),
                self.get_duration_str(sample_cnt),
                self.tks.get_input_file(),
                self.tks.get_interval_secs(),
                self.get_primary_exchange_str(),
                self.get_multiplier_str()))
        return out


class fx(asset):
    def get_collect_cmds(self):
        return ["%s -currency %s" % (cmd, self.get_currency()) for cmd in super(fx, self).get_collect_cmds()]

    def get_symbol(self):
        return self.tks.get_fx_currency_1()

    def get_currency(self):
        return self.tks.get_fx_currency_2()
        

class futures(asset):

    atts = None
    commodity = None

    def get_collect_cmds(self):
        return ["%s -expiry %s -exchange %s -currency %s" % (
            cmd,
            self.get_expiry(),
            self.get_exchange(),
            self.get_currency()) for cmd in super(futures, self).get_collect_cmds()]

    def get_atts(self):
        if (self.atts == None) or (self.commodity != self.tks.get_commodity()):
            self.commodity = self.tks.get_commodity()
            self.atts = self.tks.get_dbh().get_row_assoc(
                "SELECT exchange," +
                "currency," +
                "UNIX_TIMESTAMP(from_time) as unix_from_time," +
                "UNIX_TIMESTAMP(to_time) as unix_to_time," +
                "TIME_FORMAT(TIME(to_time), '%s') as to_time,"
                % ('%H:%i:%s') +
                "TIME_FORMAT(TIME(from_time), '%s') as from_time "
                % ('%H:%i:%s') +
                "FROM %s.futures " % (self.tks.get("mysql", "default_db")) +
                "WHERE id='%s'" % (self.tks.get_commodity()))
        return self.atts

    def get_constraint(self, **kwargs):
        last_trade_date = self.tks.get_dbh().get_one(
          "SELECT DATE(last_trade_date) " +
          "FROM unicycle.futures_roll_dates WHERE id='%s' " % (self.tks.get_commodity()) +
          "AND IFNULL(contract, DATE_FORMAT(last_trade_date,'%s'))='%s' " % ("%Y%m", self.tks.get_expiry()) + 
          "AND %s" % ("ignore_period = 0" if kwargs.has_key('ignore') and kwargs['ignore'] else "1=1"))
        last_trade_dt = mod_unicycle.get_dt_from_ts(last_trade_date if not last_trade_date is None else "%s01" % (self.tks.get_expiry()))
        if datetime.datetime.now() > last_trade_dt:
            return ("(DATE(ts) >= '%s') " % (self.tks.get_dbh().get_date_add(last_trade_dt.date(), "-1 YEAR")) +
                    "AND (DATE(ts) <= '%s')" % (last_trade_dt.date()))
        return "1=1"
        
    def get_currency(self):
        return self.get_atts()['currency']
        
    def get_exchange(self):
        return self.get_atts()['exchange']
        
    def get_expiry(self):
        return self.tks.get_expiry()
        
    def get_to_time(self):
        return self.get_atts()['to_time']
        
    def get_from_time(self):
        return self.get_atts()['from_time']
        
    def get_unix_from_time(self):
        return self.get_atts()['unix_from_time']
        
    def get_unix_to_time(self):
        return self.get_atts()['unix_to_time']
        
    def get_symbol(self):
        return self.tks.get_commodity()

class collect():
    unicycle = None
    tks = None
    table = None
    unicycle_config = None
    config = None
    exchange = None
    date_str = None
    asset = None
    pexch_file = None
    pexch = None
    mult_file = None
    mult = None

    def __init__(self, pexch_file=None, mult_file=None):
        self.pexch_file = pexch_file
        self.mult_file = mult_file

    def get_pexch(self):
        if self.pexch is None:
            if not self.pexch_file is None:
                for line in open(self.pexch_file, 'r'):
                    if self.pexch is None:
                        self.pexch = {}
                    atts = line.strip().split('\t')
                    self.pexch[atts[0]] = atts[1]
        return self.pexch

    def get_mult(self):
        if self.mult is None:
            if not self.mult_file is None:
                for line in open(self.mult_file, 'r'):
                    if self.mult is None:
                        self.mult = {}
                    atts = line.strip().split('\t')
                    self.mult[atts[0]] = atts[1]
        return self.mult

    def init(self, db_tbl, date_str):
        self.get_unicycle(self.get_unicycle_config().get_key("exchange_asset", mod_tks.get_asset_from_db_tbl(db_tbl)))
        self.get_date_str(date_str)
        self.get_table(mod_tks.get_table_from_db_tbl(db_tbl))
        self.get_asset().set_date(self.get_date_str())

    def execute(self, db_tbl, date_str, max_tries, dryrun=False, replace=False):
        self.init(db_tbl, date_str)

        done_cnt = 0
        for ii, command in enumerate(self.get_commands()):
            self.get_tks().set_feed_index(ii)
            if dryrun:
                print command
            else:
                done = False
                while not done:
                    done = True

                    if replace:
                        if os.path.exists(self.get_tks().get_saved_link_file()):
                            os.remove(self.get_tks().get_saved_link_file())
                        if (os.path.exists(self.get_tks().get_saved_input_file())):
                            os.remove(self.get_tks().get_saved_input_file())
                        if os.path.exists(self.get_tks().get_input_file()):
                            os.remove(self.get_tks().get_input_file())

                    if replace or ((not self.get_tks().is_collected()) and self.get_tks().is_try_cnt_OK(max_tries) and self.get_tks().is_IB_error_log_OK()):
                        self.unicycle.min_sleep(1.0)
                        p_child = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE)
                        self.unicycle.min_sleep(float(self.unicycle.get("IB", "minimum_request_time_seconds")) - 1.0)

                        if (p_child.poll() == None):
                            warnings.warn("Child process still running... communicating EXIT")
                            p_child.communicate("EXIT\000")
                        else:
                            if (p_child.poll() == 0):
                                if (os.path.exists(self.get_tks().get_input_file())) and (not os.path.getsize(self.get_tks().get_input_file()) == 0):
                                    self.get_tks().save_input_file()
                            else:
                                warnings.warn("Child process return code: %d, IB error: %d" % (p_child.poll(), self.get_tks().get_IB_error_code(p_child.poll())))
                                error_code = self.get_tks().get_IB_error_code(p_child.poll())
                                if ((error_code == 326) or
                                    (error_code == 502) or
                                    (error_code == 1100) or
                                    (error_code == 2105) or
                                    (error_code == 2110)):
                                    warnings.warn("Couldn't connect to TWS.")
                                elif ((error_code == 162) or
                                      (error_code == 166) or
                                      (error_code == 200)):
                                    warnings.warn("IB error is believed to be expected, saving file to prevent additional collection.")
                                    self.get_tks().insert_IB_error_table(error_code)
                                    self.get_tks().save_input_file()
                        if done:
                            done_cnt += 1

        if done_cnt == len(self.get_commands()):
            self.get_tks().increment_collect_try_cnt()

    def get_commands(self):
        return self.get_asset().get_collect_cmds()

    def get_asset(self):
        return self.asset

    def create_asset(self, tks, date=None):
        if tks.get_exchange() == "equities":
            return asset(tks, self.get_pexch(), self.get_mult(), date=date)
        if tks.get_exchange() == "futures":
            return futures(tks, self.get_pexch(), self.get_mult(), date=date)
        return fx(tks, self.get_pexch(), self.get_mult(), date=date)

    def get_date_str(self, date_str=None):
        if self.date_str is None or (self.date_str != date_str and not date_str is None):
            date_str = re.sub("\D", "", date_str)
            self.date_str = date_str
            self.get_tks().set_date_str(date_str=date_str)
        return self.date_str

    def get_table(self, table):
        if self.table is None or self.table != table:
            self.table = table
            self.get_tks().set_table(table=table)
        return self.table

    def get_unicycle(self, exchange):
        if self.unicycle is None or self.exchange != exchange:
            self.unicycle = mod_unicycle.new(config_filename="%s/%s" % (os.getenv("UNICYCLE_HOME"), self.get_config().get("exchange_config", exchange)))
            self.get_tks().set_unicycle(self.unicycle)
            self.asset = self.create_asset(self.get_tks())
            self.exchange = exchange
        return self.unicycle

    def get_config(self):
        if self.config is None:
            self.config = self.get_unicycle_config().get_config()
        return self.config

    def get_unicycle_config(self):
        if self.unicycle_config is None:
            self.unicycle_config = mod_unicycle.new()
        return self.unicycle_config

    def get_tks(self):
        if self.tks is None:
            self.tks = mod_tks.new(interval=self.get_config().get("hires", "interval"))
        return self.tks

class TWS():
    autologin_subprocess = None
    config = None

    def __init__(self, auto_login=False, config=None, timeout=None, sleep=None):
        self.config = config if not config is None else mod_unicycle.new().get_config()
        self.timeout = timeout if not timeout is None else int(self.get_config().get("IB", "auto_login_timeout"))
        self.sleep = sleep if not sleep is None else float(self.get_config().get("IB", "auto_login_sleep"))

        if auto_login and self.get_autologin_pid() is None:
            while True:
                if not self.is_ready(self.timeout):
                    warnings.warn("Could not auto start and login to TWS")
                time.sleep(self.sleep)

    def get_config(self):
        return self.config

    def is_ready(self, timeout=None):
        if not self.get_autologin_pid() == None:
            if self.is_connected(timeout=3):
                return True
            else:
                self.kill_autologin_pid()

        self.clear_autologin_subprocess()
        self.get_autologin_subprocess()
        elapsed_time = 0
        while (timeout == None) or (elapsed_time < timeout):
            if self.is_connected():
                return True
            else:
                elapsed_time += 3
                print "[ib] Elapsed time: %d of %d seconds" % (elapsed_time, timeout)
                time.sleep(3)
        self.get_autologin_subprocess().kill()
        return False

    def is_connected(self, timeout=None):
        child = subprocess.Popen(self.get_config().get("IB", "check_connection"), shell=True)
        elapsed_time = 0
        while (timeout == None) or (elapsed_time < timeout):
            if (child.poll() != None):
                print "[check_connection] exit: %d" % child.poll()
                return child.poll() == 0
            else:
                elapsed_time += 1
                time.sleep(1)
        child.kill()
        return False
                
    def clear_autologin_subprocess(self):
        self.autologin_subprocess = None

    def get_autologin_subprocess(self):
        if self.autologin_subprocess == None:
            self.autologin_subprocess = subprocess.Popen("%s -username %s -password %s -ib_url %s -tws_dir %s"
                                                         % (self.get_config().get("IB", "auto_login"),
                                                            self.get_config().get("IB", "tws_username"),
                                                            self.get_config().get("IB", "tws_passwd"),
                                                            self.get_config().get("IB", "tws_url"),
                                                            self.get_config().get("IB", "tws_dir")),
                                                         shell=True,
                                                         stdin=subprocess.PIPE)
        return self.autologin_subprocess

    def kill_autologin_pid(self):
        if not self.get_autologin_pid() == None:
            os.system("kill -TERM %s" % self.get_autologin_pid())

    def get_autologin_pid(self):
        return mod_unicycle.get_pid_by_grep("'%s'" % self.get_config().get("IB", "auto_login"), "-v grep")
