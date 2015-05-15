#!/usr/bin/env python
import mod_unicycle
import sys

# user input arguments
tblname = sys.argv[1]
filename = sys.argv[2]

# init
unicycle = mod_unicycle.new()
dbh = unicycle.get_dbh()

# create table
dbh.drop_table(tblname)
dbh.execute("CREATE TABLE "+ tblname +" (`id` CHAR(1) NOT NULL DEFAULT '', `month` CHAR(2) NOT NULL DEFAULT '', PRIMARY KEY (`id`), INDEX month_idx (`month`)) ENGINE=MyISAM")

# load data
dbh.execute("LOAD DATA LOCAL INFILE '"+ filename +"' INTO TABLE "+ tblname)
