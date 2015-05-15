#!/usr/bin/env python
"""
NAME
        update_summary.py - create a SQL table of exchange summary statistics by instrument

SYNOPSIS
        update_summary.py exchange db.table_name

DESCRIPTION

"""
import mod_unicycle
import sys
import mod_tks

unicycle = mod_unicycle.new()
tks = mod_tks.new(unicycle=unicycle, exchange=sys.argv.pop(1), interval="1day")

tblname = tks.get_summary_tablename()

tks.get_dbh().drop_table(tblname)
tks.get_dbh().execute("""
CREATE TABLE %s (
`id` VARCHAR(12) NOT NULL DEFAULT '',
`volume` INT DEFAULT NULL,
`count` INT DEFAULT NULL,
PRIMARY KEY (`id`)
) ENGINE=MyISAM
""" % (tblname))

for table in tks.get_tables():
    tks.set_table(table=table)
    tks.get_dbh().execute("INSERT INTO %s SELECT '%s', AVG(volume), COUNT(*) FROM %s" % (tblname, tks.get_table(), tks.get_db_tbl()))
