#!/usr/bin/env python
"""
"""
import sys
import mod_tks

hi_tks = mod_tks.new(config_filename=sys.argv[1], asset=sys.argv[2], interval=sys.argv[3])
lo_tks = mod_tks.new(config_filename=sys.argv[1], asset=sys.argv[2], interval=sys.argv[4])

lores_sample_threshold = hi_tks.get_unicycle().get_dict("lores_sample_threshold")

# build new db
lo_tks.get_unicycle().get_dbh().drop_database(lo_tks.get_db())

for table in hi_tks.get_tables():

    print "Note: processing %s" % (table)
    hi_tks.set_table(table=table)
    lo_tks.set_table(table=table)
    lo_tks.replace_into(hi_db_tbl=hi_tks.get_db_tbl(), hi_interval_secs=hi_tks.get_interval_secs(), threshold=lores_sample_threshold[sys.argv[4]], default_time=hi_tks.get_config().get("lores", "1day_default_time"))
