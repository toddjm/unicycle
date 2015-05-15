#!/usr/bin/env python
import argparse
import mod_ib
import mod_tks
import mod_unicycle
import os

parser = argparse.ArgumentParser(description='Request and save resultant equities price data',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-primary_exchange_file',
                    default="%s/%s" % (os.getenv("UNICYCLE_HOME"), mod_unicycle.new().get("IB", "primary_exchange_file")),
                    help='include primary exchange values')
parser.add_argument('-date_file', help='date strings of format YYYYMMDD on separate lines')
parser.add_argument('-instrument_file', help='symbol names on separate lines with underscores for spaces')
parser.add_argument('-instrument', help='comma separated list')
parser.add_argument('-by_volume', dest='order_col', action='store_const', const='volume', default=None,
                    help='sort instruments by volume')
parser.add_argument('-by_count', dest='order_col', action='store_const', const='count', default=None,
                    help='sort instruments by count')
parser.add_argument('-reverse', dest='order_att', action='store_const', const='ASC', default='DESC',
                    help='reverse order used with -by_count or -by_volume')
parser.add_argument('-max_tries', type=int, help='maximum collection try count')
parser.add_argument('-replace', action='store_true', help='replace existing collection')
parser.add_argument('-dryrun', action='store_true', help='show collection command as if it would be executed')
parser.add_argument('dates', nargs='*')
args = parser.parse_args()

unicycle = mod_unicycle.new()
tks = mod_tks.new(unicycle=unicycle, exchange=unicycle.get_key("exchange_type", "equities"), interval=unicycle.get("hires", "interval"))
collect = mod_ib.collect(pexch_file=args.primary_exchange_file)

# dates
if args.date_file:
    args.dates = [line.strip() for line in open(args.date_file, 'r')]

if not len(args.dates):
    args.dates = [tks.get_dbh().get_date_str()]

# tables
tables = list()
if args.instrument:
    tables = ["%s_%s" % (val, 'tks') for val in args.instrument.split(',')]
elif args.instrument_file:
    tables = [tks.get_table_from_instrument(line.strip()) for line in open(args.instrument_file, 'r')]
else:
    tables = tks.get_tables()

tables = tks.get_dbh().get_list("SELECT a.id from %s a " % (tks.get_dbh().get_temp_table_from_list(tables, "VARCHAR(12)")) +
                                "LEFT JOIN %s b on (a.id=b.id) " % (tks.get_summary_tablename()) +
                                "ORDER BY %s a.id" % ("%s %s," % (args.order_col, args.order_att) if args.order_col else ""))

for date_str in args.dates:
    for ii in range(int(unicycle.get("collect", "try_loop_cnt"))):
        for table in tables:
            tks.set_table(table=table)
            collect.execute(tks.get_db_tbl(), date_str, args.max_tries, dryrun=args.dryrun, replace=args.replace)
