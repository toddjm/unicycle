#!/usr/bin/env python
"""
NAME
        delete_ts.py - remove timestamps by asset and date

SYNOPSIS
        delete_ts.py asset_name date...

DESCRIPTION
        For each interval and table at an asset level delete all timestamps that match the input dates.

        asset_name
                This should be one of the variables found in ~/unicycle/unicycle.cfg in the [asset] section.  For example:
                futures, equities, fx
"""
import sys
import mod_tks

tks = mod_tks.new(asset=sys.argv[1])

for interval in tks.get_unicycle().get_dict("interval").keys():
    print "Deleting %s_%s.%s_%s" % (sys.argv[1], interval, '*', tks.get_feed())
    tks.set_interval(interval=interval)
    for table in tks.get_tables():
        tks.set_table(table=table)
        for dt in sys.argv[2:]:
            tks.get_dbh().execute("DELETE FROM %s WHERE DATE(ts)='%s'" % (tks.get_table(), dt))
