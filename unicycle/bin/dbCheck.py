#!/usr/bin/env python
"""
NAME
        dbCheck.py - dump record count and checksum for the specified database

SYNOPSIS
        dbCheck.py database
        

DESCRIPTION
        Ouput produces a tab delimited table of table name, record count and checksum.
"""
import mod_unicycle
import sys

unicycle = mod_unicycle.new()

# set database
unicycle.get_dbh().execute("USE %s" % (sys.argv[1]))

# build sorted table list
tables = unicycle.get_dbh().get_list("SHOW TABLES")
tables.sort()

checksums = list()
counts = list()

print "Table count for database %s: %d" % (sys.argv[1], len(tables))

for table in tables:
    print "CHECKSUM TABLE %s" % (table)
    checksums.append(unicycle.get_dbh().get_row("CHECKSUM TABLE %s" % (table))[1])

for table in tables:
    print "SELECT COUNT(*) FROM %s" % (table)
    counts.append(unicycle.get_dbh().get_one("SELECT COUNT(*) FROM %s" % (table)))

for ii in range(len(tables)):
    print "%s\t%s\t%s" % (tables[ii], counts[ii], checksums[ii])
