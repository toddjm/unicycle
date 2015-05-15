#!/usr/bin/env python
"""valid_ts.py

./valid_ts.py <config_file>

Creates a valid timestamp table 'db'.'tblname' within 'from_date' and 'to_date' excluding lists of 'exclude_days',
and 'holidays'.  Additionally timestamps are removed based on 'half_days' and intraday 'from_time'
and 'to_time'.

'time_zone' specifies the times used in the config file.

All config variables should be in section [this].

"""
import sys
import mod_unicycle
import mod_tks

unicycle = mod_unicycle.new(sys.argv[1])
dbh = unicycle.get_dbh()
config = unicycle.get_config()
tblname = "%s.%s" % (unicycle.get("mysql", "default_db"), mod_unicycle.get_valid_table(exchange=config.get("this","exchange"), interval=config.get("hires", "interval")))

print "create table..."
dbh.drop_table(tblname)
dbh.execute("CREATE TABLE %s (`ts` TIMESTAMP NOT NULL DEFAULT 0, PRIMARY KEY (`ts`)) ENGINE=MyISAM" % (tblname))
print "insert into table from global valid..."
dbh.execute("INSERT INTO %s SELECT ts FROM %s WHERE ts >= '%s' AND ts < '%s'" %
            (tblname, config.get("config", "valid_ts"), config.get("this","from_date") + " 00:00:00", config.get("this","to_date") + " 00:00:00"))

print "exclude_days:"
exclude_days = filter(None, config.get("this", "exclude_days").split('\n'))
for day in exclude_days:
    print day
    dbh.execute("delete from "+tblname+" where dayofweek(ts)='"+str(dbh.get_dayofweek_index(day))+"'")
    
print "holidays:"
holidays = filter(None, config.get("this", "holidays").split('\n'))
for holiday in holidays:
    print holiday
    dbh.execute("delete from "+tblname+" where date(ts)='"+holiday+"'")
    
print "from_time: "+config.get("this","from_time")
dbh.execute("delete from "+tblname+" where time(ts)<'"+config.get("this","from_time")+"'")
print "to_time: "+config.get("this","to_time")
dbh.execute("delete from "+tblname+" where time(ts)>='"+config.get("this","to_time")+"'")

print "half_days:"
half_days = filter(None, config.get("this", "half_days").split('\n'))
for half_day in half_days:
    flds = half_day.split('\t')
    print flds[0], flds[1], flds[2]
    dbh.execute("delete from "+tblname+" where date(ts)='"+flds[0]+"' and ts <'"+flds[0]+" "+flds[1]+"'")
    dbh.execute("delete from "+tblname+" where date(ts)='"+flds[0]+"' and ts >='"+flds[0]+" "+flds[2]+"'")

intervals = unicycle.get_dict("interval").keys()
intervals.remove(config.get("hires", "interval"))
lo_tks = mod_tks.new(unicycle=unicycle)

default_time = dbh.get_converted_time(config.get("lores", "1day_default_time"),
                                      config.get("lores", "1day_time_zone"),
                                      config.get("this", "time_zone"))

for interval in intervals:
    print interval

    lo_tks.set_interval(interval=interval)
    lo_tks.load_interval_table(tblname, db=config.get("this", "db"))

    lo_tblname = "%s.%s" % (unicycle.get("mysql", "default_db"), mod_unicycle.get_valid_table(exchange=config.get("this","exchange"), interval=interval))
    dbh.drop_table(lo_tblname)
    dbh.execute("CREATE TABLE %s (`ts` TIMESTAMP NOT NULL DEFAULT 0, `hi_count` INT DEFAULT NULL, PRIMARY KEY (`ts`), INDEX `hi_count_idx` (`hi_count`)) ENGINE=MyISAM" % (lo_tblname))
    dbh.execute("INSERT INTO %s SELECT MIN(a.ts), COUNT(*) FROM %s a GROUP BY DATE(a.ts), period" % (lo_tblname, lo_tks.get_interval_table()))

    if (interval == "1day"):
        dbh.execute("UPDATE %s SET ts=TIMESTAMP(CONCAT(DATE_FORMAT(ts, '%s'), ' %s'))" % (lo_tblname, '%Y-%m-%d', default_time))
