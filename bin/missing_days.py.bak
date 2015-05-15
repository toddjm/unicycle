#!/usr/bin/env python
import mod_unicycle
import mod_tks
import sys

unicycle = mod_unicycle.new(config_filename=sys.argv[1])
tks = mod_tks.new(unicycle=unicycle, asset=unicycle.get("this", "default_asset"), interval=unicycle.get("this", "interval"))

# build table of valid dates from from_date to to_date
tks.get_dbh().execute("CREATE TEMPORARY TABLE days (`ts` TIMESTAMP NOT NULL DEFAULT 0, PRIMARY KEY(`ts`)) ENGINE=MYISAM")
tks.get_dbh().execute("INSERT INTO days SELECT DATE(ts) FROM %s WHERE DATE(ts) >= '%s' AND DATE(ts) < '%s' GROUP BY DATE(ts)"
                      % ("%s.%s" % (tks.get_config().get("mysql", "default_db"), mod_unicycle.get_valid_table(exchange=unicycle.get_key("exchange_asset", unicycle.get("this", "default_asset")), interval=tks.get_config().get("this", "interval"))),
                         tks.get_config().get("this", "from_date"),
                         tks.get_config().get("this", "to_date")))

for signal in tks.get_unicycle().get_list("this", "signals"):

    # set db_tbl
    atts = signal.split('\t')
    tks.set_asset(asset=atts[0])
    tks.set_table(table=atts[1])

    # get dates where is no link
    for rec in tks.get_dbh().get_all("SELECT DATE_FORMAT(d.ts, '%s') FROM days d LEFT JOIN %s a on DATE(a.ts)=DATE(d.ts) WHERE a.ts IS NULL" % ('%Y-%m-%d', tks.get_table())):
        print "%s\t%s" % (tks.get_instrument(), rec[0])
