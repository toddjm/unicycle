#!/usr/bin/env python
"""update futures roll dates.

"""

import mod_unicycle
import os
import sys
import warnings


def get_adjusted_date(datestr):
    """get adjusted date."""
    if datestr == None:
        return "NULL"
    while True:
        adj_datestr = datestr
        datestr = dbh.get_next_day(datestr)
#        if valid_dates.has_key(datestr):
        if datestr in valid_dates:
            return "'" + adj_datestr + "'"

# user input arguments
cfg = sys.argv[1]  # unicycle.cfg
tblname = sys.argv[2]  # futures_roll_dates
filename = sys.argv[3]  # roll_dates.txt

# init
unicycle = mod_unicycle.new(cfg)
dbh = unicycle.get_dbh()
tmp_filename = "/tmp/%s.txt" % (tblname)  # /tmp/futures_roll_dates.txt
valid_tblname = unicycle.get("feed", "valid_ts_tblname")  # futures_valid_15sec

# drop table
print "Drop: %s" % tblname
dbh.drop_table(tblname)  # drop unicycle.futures_roll_dates

# create table
print "Create: %s" % tblname
dbh.execute("""
CREATE TABLE %s (`id` VARCHAR(8) NOT NULL DEFAULT '',
`contract` VARCHAR(6) DEFAULT NULL,
`ignore_period` TINYINT(1) DEFAULT 0,
`first_trade_date` TIMESTAMP NOT NULL DEFAULT 0,
`last_trade_date` TIMESTAMP NOT NULL DEFAULT 0,
`roll_date` TIMESTAMP NULL DEFAULT NULL,
`adj_last_trade_date` TIMESTAMP NOT NULL DEFAULT 0,
`adj_roll_date` TIMESTAMP NULL DEFAULT NULL,
INDEX idx (`id`),
INDEX ignore_period_idx (`ignore_period`)) ENGINE=MyISAM
""" % tblname)

# remove blank lines from file
os.system("grep -E '^[A-Z]' " + filename + " > " + tmp_filename)
datefmt = "%Y%m%d"

# ignore MySQLdb warnings caused by roll_date being null in input file
warnings.filterwarnings('ignore', '.*doesn\'t contain data for all columns.*')

# ignore MySQLdb warnings caused by roll_date being null in input file
warnings.filterwarnings('ignore', '.*Data truncated for column.*')

# load data
print "Load: %s" % tblname

dbh.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s (id, contract, "
            "ignore_period, first_trade_date, last_trade_date, roll_date)" %
            (tmp_filename, tblname))

print "Prep: %s" % tblname
dbh.execute("UPDATE %s SET contract=NULL WHERE LENGTH(contract) <> 6" % tblname)

# null roll dates of zero
#dbh.execute("UPDATE " + tblname + "set roll_date=NULL where roll_date=0")
dbh.execute("UPDATE %s set roll_date=NULL where roll_date=0" % tblname)

# check if the newest last_trade_date is newer than the newest valid date
tf_trade_date = dbh.get_one("SELECT DATE_FORMAT(DATE(last_trade_date), '%s') "
                            "FROM %s ORDER BY last_trade_date DESC" %
                            (datefmt, tblname))

tf_valid_date = dbh.get_one("SELECT DATE_FORMAT(DATE(ts), '%s') FROM %s "
                            "ORDER BY ts DESC" % (datefmt, valid_tblname))

if tf_trade_date > tf_valid_date:
    raise StandardError("Farthest out last_trade_date: '%s' is greater "
                        "than farthest out valid date: '%s'\n" %
                        (tf_trade_date, tf_valid_date))

tf_trade_date = dbh.get_one("SELECT DATE_FORMAT(DATE(first_trade_date), '%s') "
                            "FROM %s ORDER BY first_trade_date DESC" %
                            (datefmt, tblname))

if tf_trade_date > tf_valid_date:
    raise StandardError("Farthest out first_trade_date: '%s' is greater "
                        "than farthest out valid date: '%s'\n" %
                        (tf_trade_date, tf_valid_date))

# compute adjusted dates
valid_dates = dbh.get_dict("SELECT DATE_FORMAT(DATE(ts), '%s') AS day, 1 "
                           "FROM %s GROUP BY day" %
                           ("%Y-%m-%d", valid_tblname))

dbh.execute("SELECT id, DATE_FORMAT(last_trade_date, '%s'), "
            "DATE_FORMAT(last_trade_date, '%s'), DATE_FORMAT(roll_date, "
            "'%s') FROM %s" % ("%Y-%m-%d %H:%i:%s", datefmt, datefmt,
                               tblname))

for rec in dbh.cursor.fetchall():
    print "Adjust: %s %s" % (rec[0], rec[2])

#     print("UPDATE %s SET adj_last_trade_date=%s, adj_roll_date=%s
#     WHERE id='%s' AND last_trade_date='%s'" % (tblname,
#     get_adjusted_date(rec[2]), get_adjusted_date(rec[3]), rec[0], rec[1]))
    dbh.execute("UPDATE %s SET adj_last_trade_date=%s, adj_roll_date=%s "
                "WHERE id='%s' AND last_trade_date='%s'" %
                (tblname, get_adjusted_date(rec[2]),
                 get_adjusted_date(rec[3]), rec[0], rec[1]))
