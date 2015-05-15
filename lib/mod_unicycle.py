"""unicycle

"""

import ConfigParser
import datetime
import mod_sql
import os
import re
import subprocess
import sys
import time


def get_dt_from_ts(timestamp):
    """Get dt from timestamp."""
    value = re.sub('\D', '', str(timestamp))
    if len(value) == 8:
        value += "000000"
    return datetime.datetime(int(value[0:4]),
                             int(value[4:6]),
                             int(value[6:8]),
                             int(value[8:10]),
                             int(value[10:12]),
                             int(value[12:14]))


def get_arg_str(feed_args, quote_char="\""):
    """Get arg string."""
    out = list()
    for arg in feed_args:
        out.append("%s%s%s" % (quote_char, arg, quote_char))
    return ",".join(out)


def classname(obj):
    """classname."""
    return obj.__class__.__name__


def is_integer(obj):
    """is integer?"""
#    try:
#        int(obj)
#        return True
#    except:
#        return False
    return isinstance(obj, int)


def binstr(num):
    """binstr."""
    if num <= 0:
        return str(num)
    else:
        return bin(num).lstrip('0b')


def get_valid_table(exchange=None, interval=None):
    """Get valid table."""
    return "%s_valid_%s" % (exchange, interval)


def get_pid_by_grep(*srch_strings):
    """Get PID by grep."""
    grep_list = list()
    for string in srch_strings:
        grep_list.append("grep %s" % string)
    if sys.version_info < (2, 7):
        pid = os.popen("ps x | %s | awk '{print $1}'"
                       % (" | ".join(grep_list))).readline().strip()
    else:
        pid = subprocess.check_output("ps x | %s | awk '{print $1}'"
                                      % (" | ".join(grep_list)),
                                      shell=True).strip()
#        pid = subprocess.check_output("ps x | %s | awk '{print $1}'"
#                                      % (" | ".join(grep_list)),
#                                      shell=True)
#        pid = [i.strip() for i in pid]
    return pid if not pid == '' else None


class new():
    """new class."""
    autoset_time_zone = None
    basename = "unicycle.cfg"
    config_filename = None
    config_filelist = None
    config_filelist_length = None
    config_append_index = None
    config = None
    dbh = None
    start_time = None

    def __init__(self, config_filename=None, autoset_time_zone=True):
        """init."""
        self.config_filename = config_filename
        self.get_config_filelist()
        self.config_filelist_length = len(self.get_config_filelist())
        self.autoset_time_zone = autoset_time_zone

    def get_dbh(self):
        """Get dbh."""
        if self.dbh == None:
            self.dbh = mod_sql.connection(
                    host=self.get_config().get("mysql", "host"),
                    user=self.get_config().get("mysql", "user"),
                    passwd=self.get_config().get("mysql", "passwd"),
                    db=self.get_config().get("mysql", "default_db"))
            if (self.autoset_time_zone and self.get_config().has_option(
                    "this", "time_zone")):
                self.dbh.set_session_time_zone(self.get_config().get(
                    "this", "time_zone"))
        return self.dbh

    def get_config(self):
        """Get config."""
        if self.config == None:
            self.config = ConfigParser.SafeConfigParser()
            self.config.read(self.get_config_filelist())
        return self.config

    def append_config_filelist(self, filename):
        """Append config file list."""
        self.config_filelist.append(filename)
        self.config = None

    def set_local_cfg_file(self, filename):
        """Set local cfg file."""
        del self.config_filelist[len(self.config_filelist):]
        self.append_config_filelist(filename)

    def get_array(self, section, name, separator='\t'):
        """Get array."""
        return [line.split(separator) for line in
                self.get_list(section, name)]

    def get_list(self, section, name):
        """Get list."""
#    def get_list(self, section, name, separator='\n'):
#        """Get list."""
        if not self.get_config().has_option(section, name):
            return []
#        return filter(None,
#                      self.get_config().get(section, name).split(separator))
        values = self.get_config().get(section, name).split('\n')
        values = [i for i in values if i]
        return values

    def safe_get(self, section, name):
        """Safe get."""
        if self.get_config().has_option(section, name):
            return self.get(section, name)
        else:
            return None

    def get(self, section, name):
        """Get."""
        return self.get_config().get(section, name)

    def get_dict(self, section):
        """Get dict."""
        values = {}
        for item in self.get_config().items(section):
            values[item[0]] = item[1]
        return values

    def get_key(self, section, value):
        """Get key."""
        section = self.get_dict(section)
        return section.keys()[section.values().index(value)]

    def get_value(self, section, key):
        """Get value."""
        return self.get(section, key)

    def get_sorted_numerical_values(self, section):
        """Get sorted numerical values."""
        values = list()
        for item in self.get_config().items(section):
            values.append(int(item[1]))
        values.sort()
        return values

    def get_config_filelist(self):
        """Get config file list."""
        if (self.config_filelist == None):
            files = list()
            files.append(self.get_config_file(
                    dirname=os.getenv("UNICYCLE_HOME")))
            files.append(self.get_config_file(dirname=os.getenv("HOME")))
            if self.config_filename != None:
                if classname(self.config_filename) == "str":
                    files.append(self.config_filename)
                else:
                    # assume is a list
                    files += self.config_filename
            self.config_filelist = files
        return self.config_filelist

    def get_default_cfg_file(self):
        """Get default cfg file."""
        return self.basename

    def get_config_file(self, dirname="."):
        """Return config file pathname from dirname."""
        return dirname + "/" + self.get_default_cfg_file()

    def min_sleep(self, min_delay):
        """Min sleep."""
        if not self.start_time == None:
            sleep_time = float(min_delay) - (time.time() - self.start_time)
            if sleep_time > 0:
                print "sleep_time: %f" % (sleep_time)
                time.sleep(sleep_time)
        self.start_time = time.time()
