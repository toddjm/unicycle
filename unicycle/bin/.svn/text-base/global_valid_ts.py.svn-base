#!/usr/bin/env python
"""
"""
import mod_unicycle
import sys

unicycle = mod_unicycle.new()
from_date_list = list()
to_date_list = list()
tbl = unicycle.get_config().get("config", "valid_ts")
interval = int(unicycle.get_config().get("interval", unicycle.get_config().get("hires", "interval")))

for cfg in sys.argv[1:]:
    unicycle.set_local_cfg_file(cfg)
    from_date_list.append(unicycle.get_config().get("this", "from_date"))
    to_date_list.append(unicycle.get_config().get("this", "to_date"))

from_date_list.sort()
to_date_list.sort()

from_date = from_date_list[0]
to_date = to_date_list[-1]

unicycle.get_dbh().set_session_time_zone(unicycle.get_config().get("lores", "1day_time_zone"))

start = unicycle.get_dbh().get_one("select unix_timestamp('%s')" % (from_date))
stop = unicycle.get_dbh().get_one("select unix_timestamp('%s')" %  (to_date))

if (unicycle.get_dbh().table_exists(tbl)):
    if (str(unicycle.get_dbh().get_one("SELECT DATE(ts) from %s ORDER BY ts LIMIT 1" % (tbl))) == from_date):
        if (str(unicycle.get_dbh().get_one("SELECT DATE(ADDTIME(ts, '0 0:0:%d')) from %s ORDER BY ts DESC LIMIT 1" % (interval, tbl))) == to_date):
            exit(0)

fp = open("/tmp/%s.txt" % (tbl), 'w')

for sec in range(start, stop, interval):
    fp.write("%d\n" % (sec))

fp.close()

unicycle.get_dbh().execute("DROP TABLE IF EXISTS %s" % (tbl))
unicycle.get_dbh().execute("CREATE TABLE %s (`ts` TIMESTAMP NOT NULL DEFAULT 0, PRIMARY KEY (`ts`)) ENGINE=MyISAM" % (tbl))
unicycle.get_dbh().execute("LOAD DATA LOCAL INFILE '/tmp/%s.txt' INTO TABLE %s (@uts) SET ts=FROM_UNIXTIME(@uts)" % (tbl, tbl))
