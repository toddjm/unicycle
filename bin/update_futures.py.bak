#!/usr/bin/env python
import mod_unicycle
import sys

unicycle = mod_unicycle.new(sys.argv[1])
dbh = unicycle.get_dbh()
dbh.set_session_time_zone(unicycle.config.get("feed","time_zone"))

tblname = sys.argv[2]
filename = sys.argv[3]
exch_filename = sys.argv[4]

fp = open(exch_filename)

exchanges = list()
for line in fp:
    exchanges.append("'"+ line.strip() + "'")

# create table
dbh.drop_table(tblname)
dbh.execute("""
CREATE TABLE %s (`id` VARCHAR(8) NOT NULL DEFAULT '',
`exchange` ENUM(%s) NOT NULL,
`from_time` TIMESTAMP NOT NULL DEFAULT 0,
`to_time` TIMESTAMP NOT NULL DEFAULT 0,
`currency` VARCHAR(3) NOT NULL,
PRIMARY KEY (`id`),
INDEX exchange_idx (`exchange`)) ENGINE=MyISAM
""" % (tblname, ",".join(exchanges)))

# load data
dbh.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s (id, exchange, @t0, @t1, currency) SET from_time = CONCAT('1970-01-01 ', @t0), to_time = CONCAT('1970-01-01 ', @t1)" % (filename, tblname))
