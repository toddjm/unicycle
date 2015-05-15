"""unicycle

"""

import ConfigParser
import datetime
import os
import re
import mod_sql
import subprocess
import sys
import time


def get_dt_from_ts(ts):
    s = re.sub('\D', '', str(ts))
    if len(s) == 8:
        s += "000000"
    return datetime.datetime(int(s[0:4]),
                             int(s[4:6]),
                             int(s[6:8]),
                             int(s[8:10]),
                             int(s[10:12]),
                             int(s[12:14]))


def get_arg_str(feed_args,
                quote_char="\""):
    out = list()
    for arg in feed_args:
        out.append(
          "%s%s%s"
          % (
          quote_char,
          arg,
          quote_char))
    return ",".join(out)


def classname(obj):
    return obj.__class__.__name__


def is_integer(obj):
    try:
        int(obj)
        return True
    except:
        return False


def binstr(num):
    if num <= 0:
        return str(num)
    else:
        return bin(num).lstrip('0b')


def get_valid_table(exchange=None,
                    interval=None):
    return "%s_valid_%s" % (
      exchange,
      interval)


def get_pid_by_grep(*srch_strings):
    grep_list = list()
    for string in srch_strings:
        grep_list.append(
          "grep %s"
          %
          string)
    if sys.version_info < (2, 7):
        pid = os.popen("ps x | %s | awk '{print $1}'"
                       % (" | ".join(grep_list))).readline().strip()
    else:
        pid = subprocess.check_output(
                "ps x | %s | awk '{print $1}'"
                % (
                " | ".join(grep_list)),
                shell=True).strip()
    return pid if not pid == '' else None


class new():

    autoset_time_zone = None
    basename = "unicycle.cfg"
    config_filename = None
    config_filelist = None
    config_filelist_length = None
    config_append_index = None
    config = None
    dbh = None
    start_time = None

    def __init__(self,
                 config_filename=None,
                 autoset_time_zone=True):
        self.config_filename = config_filename
        self.get_config_filelist()
        self.config_filelist_length = len(self.get_config_filelist())
        self.autoset_time_zone = autoset_time_zone

    def get_dbh(self):
        if (self.dbh == None):
            self.dbh = mod_sql.connection(
              host=self.get_config().get("mysql", "host"),
              user=self.get_config().get("mysql", "user"),
              passwd=self.get_config().get("mysql", "passwd"),
              db=self.get_config().get("mysql", "default_db"))
            if (self.autoset_time_zone and self.get_config().has_option(
              "this",
              "time_zone")):
                self.dbh.set_session_time_zone(
                  self.get_config().get(
                  "this",
                  "time_zone"))
        return self.dbh

    def get_config(self):
        if (self.config == None):
            self.config = ConfigParser.SafeConfigParser()
            self.config.read(self.get_config_filelist())
        return self.config

    def append_config_filelist(self,
                               filename):
        self.config_filelist.append(filename)
        self.config = None

    def set_local_cfg_file(self,
                           filename):
        del self.config_filelist[len(self.config_filelist):]
        self.append_config_filelist(filename)

    def get_array(self,
                  section,
                  name,
                  separator='\t'):
        return [line.split(separator) for line in self.get_list(section, name)]

    def get_list(self,
                 section,
                 name,
                 separator='\n'):
        if not self.get_config().has_option(section, name):
            return []
        return filter(None, self.get_config().get(section, name).split(separator))

    def safe_get(self,
                 section,
                 name):
        if self.get_config().has_option(section, name):
            return self.get(section, name)
        else:
            return None

    def get(self,
            section,
            name):
        return self.get_config().get(section, name)

    def get_dict(self,
                 section):
        values = {}
        for item in self.get_config().items(section):
            values[item[0]] = item[1]
        return values

    def get_key(self,
                section,
                value):
        section = self.get_dict(section)
        return section.keys()[section.values().index(value)]

    def get_value(self,
                  section,
                  key):
        return self.get(section, key)

    def get_sorted_numerical_values(self,
                                    section):
        values = list()
        for item in self.get_config().items(section):
            values.append(int(item[1]))
        values.sort()
        return values

    def get_config_filelist(self):
        if (self.config_filelist == None):
            files = list()
            files.append(
              self.get_config_file(
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
        return self.basename

    def get_config_file(self,
                        dirname="."):
        """Return config file pathname from dirname."""
        return dirname + "/" + self.get_default_cfg_file()

    def min_sleep(self,
                  min_delay):
        if not self.start_time == None:
            sleep_time = float(min_delay) - (time.time() - self.start_time)
            if sleep_time > 0:
                print "sleep_time: %f" % (
                  sleep_time)
                time.sleep(sleep_time)
        self.start_time = time.time()
