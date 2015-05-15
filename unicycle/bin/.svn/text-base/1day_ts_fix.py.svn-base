#!/usr/bin/env python
import sys
import mod_tks

tks = mod_tks.new(interval="1day")

for asset in sys.argv[1:]:
    print "Asset: %s" % (asset)
    tks.set_asset(asset=asset)
    for table in tks.get_tables():
        print table
        tks.set_table(table=table)
        tks.get_dbh().execute("UPDATE %s SET ts=TIMESTAMP(CONCAT(DATE_FORMAT(ts, '%s'), ' %s'))" % (tks.get_table(), '%Y-%m-%d', tks.get_config().get("lores", "1day_default_time")))
