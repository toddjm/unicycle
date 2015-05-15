#!/usr/bin/env python
"""collect equities.

"""

import argparse
import mod_ib
import mod_tks
import mod_unicycle
import os

PARSER = argparse.ArgumentParser(
        description='Request and save resultant equities price data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#PARSER.add_argument('-primary_exchange_file',
#                    default="%s/%s" %
#                    (os.getenv("UNICYCLE_HOME"),
#                     mod_unicycle.new().get("IB",
#                                            "primary_exchange_file")),
#                    help='include primary exchange values')
PARSER.add_argument('-primary_exchange_file',
                    default="%s/%s" %
                    (os.getenv("UNICYCLE_HOME"),
                     mod_unicycle.new().get("primary_exchange_file",
                                            "equities")),
                    help='include primary exchange values')

PARSER.add_argument('-date_file',
                    help='date strings of format YYYYMMDD on separate lines')

PARSER.add_argument(
        '-instrument_file',
        help='symbol names on separate ' 'lines with underscores for spaces')

PARSER.add_argument('-instrument', help='comma separated list')

PARSER.add_argument('-max_tries', type=int,
                    help='maximum collection try count')

PARSER.add_argument('-replace', action='store_true',
                    help='replace existing collection')

PARSER.add_argument('-dryrun', action='store_true',
                    help='show collection command as if it would be executed')

PARSER.add_argument('dates', nargs='*')

ARGS = PARSER.parse_args()

UNICYCLE = mod_unicycle.new()
TKS = mod_tks.new(unicycle=UNICYCLE,
                  exchange=UNICYCLE.get_key("exchange_type", "equities"),
                  interval=UNICYCLE.get("hires", "interval"))
COLLECT = mod_ib.collect(pexch_file=ARGS.primary_exchange_file)

if ARGS.date_file:
    ARGS.dates = [line.strip() for line in open(ARGS.date_file, 'r')]

if not ARGS.dates:
    ARGS.dates = [TKS.get_dbh().get_date_str()]

TABLES = list()
if ARGS.instrument:
    TABLES = ["%s_%s" % (val, 'tks') for val in ARGS.instrument.split(',')]
elif ARGS.instrument_file:
    TABLES = [TKS.get_table_from_instrument(line.strip())
              for line in open(ARGS.instrument_file, 'r')]
else:
    TABLES = TKS.get_tables()

TABLE_LIST = TKS.get_dbh().get_temp_table_from_list(TABLES, "VARCHAR(12)")
TABLES = TKS.get_dbh().get_list(
        ('SELECT a.id from {0} as a LEFT JOIN {1} as b on ' +
         '(a.id=b.id) ORDER BY a.id').format(TABLE_LIST,
                                             TKS.get_summary_tablename()))

for date_str in ARGS.dates:
    for i in range(int(UNICYCLE.get("collect", "try_loop_cnt"))):
        for table in TABLES:
            TKS.set_table(table=table)
            COLLECT.execute(TKS.get_db_tbl(), date_str, ARGS.max_tries,
                            dryrun=ARGS.dryrun, replace=ARGS.replace)
