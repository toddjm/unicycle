#!/usr/bin/env python
"""collect futures."""
import argparse
import mod_ib
import mod_tks
import mod_unicycle
import os

parser = argparse.ArgumentParser(
        description='Request and save resultant futures price data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-date_file',
                    help='date strings of format YYYYMMDD on separate lines')
parser.add_argument('-multiplier_file',
                    default="%s/%s" % (os.getenv("UNICYCLE_HOME"),
                                       mod_unicycle.new().get(
                                       "multiplier_file", "futures")),
                                      help='include multiplier values')
parser.add_argument('-instrument',
                    help='comma separated list')
parser.add_argument('-expiry',
                    help='comma separated list')
parser.add_argument('-max_tries',
                    type=int,
                    help='maximum collection try count')
parser.add_argument('-dryrun',
                    action='store_true',
                    help='show collect command as it would be executed')
parser.add_argument('-replace',
                    action='store_true',
                    help='replace existing collection')
parser.add_argument('-ignore',
                    action='store_true',
                    help='skip symbols marked ignore in futures_roll_dates')
parser.add_argument('dates', nargs='*')
args = parser.parse_args()

unicycle = mod_unicycle.new()
tks = mod_tks.new(unicycle=unicycle,
                  exchange=unicycle.get_key("exchange_type", "futures"),
                  interval=unicycle.get("hires", "interval"))
collect = mod_ib.collect(mult_file=args.multiplier_file)

# dates
if args.date_file:
    args.dates = [line.strip() for line in open(args.date_file, 'r')]

if not len(args.dates):
    args.dates = [tks.get_dbh().get_date_str()]

for date_str in args.dates:
    for ii in range(int(unicycle.get("collect", "try_loop_cnt"))):
        for rec in unicycle.get_dbh().get_all(
          "SELECT id, IFNULL(contract, DATE_FORMAT(last_trade_date, "
          "'%s')) AS expiry " % ('%Y%m') +
          "FROM %s.futures_roll_dates " % unicycle.get("mysql", "default_db") +
          "WHERE %s AND " % ("ignore_period=0" if args.ignore else "1=1") +
          "%s AND " % ("(%s)" %
          (' OR '.join(["id='%s'" %
          (val) for val in args.instrument.split(',')]))
          if args.instrument else "1=1") +
          "%s AND " % ("(%s)" %
          (' OR '.join(["IFNULL(contract, DATE_FORMAT(last_trade_date, "
          "'%s'))='%s'" %
          ('%Y%m', val) for val in args.expiry.split(',')]))
          if args.expiry else "1=1") +
          "DATE(last_trade_date) >= '%s' AND " % (date_str) +
          "DATE(IF(first_trade_date, first_trade_date, '%s')) <= '%s' " %
          (unicycle.get("config",
                        "default_futures_first_trade_date"), date_str) +
          "ORDER BY expiry, id"):
            tks.set_table(table="%s%s_tks" % (rec[0], rec[1]))
            collect.execute(tks.get_db_tbl(), date_str, args.max_tries,
                            dryrun=args.dryrun, replace=args.replace)
