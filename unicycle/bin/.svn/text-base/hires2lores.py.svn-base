#!/usr/bin/env python
"""
NAME
        hires2lores.py - high resolution data to low resolution data

SYNOPSIS
        hires2lores.py [-v] config_file interval

DESCRIPTION

        Derive lower resolution data specified by interval from the highest resolution data.

"""
import argparse
import mod_unicycle
import sys
import mod_tks

parser = argparse.ArgumentParser(description='Derive low-resolution data from high-resolution data.')
parser.add_argument('-v', action='store_true', dest='verbose', help='enable verbose mode')
parser.add_argument('config_filename', metavar='config_filename',
                    help='the configuration file for the asset class')
parser.add_argument('-tbl_reg_exp', metavar='RE', help='For tables that match regular expression')
parser.add_argument('interval', metavar='interval', nargs='?',
                    choices=('15min', '1min', '1day'), default=None, 
                    help='the desired low-resolution period (default: runs all lores intervals)')
parser.add_argument('-dryrun', action='store_true', help='show collection command as if it would be executed')
args = parser.parse_args()

unicycle = mod_unicycle.new(config_filename=args.config_filename)
hi_tks = mod_tks.new(unicycle=unicycle, exchange=unicycle.get("this", "exchange"), interval=unicycle.get("hires", "interval"))
lo_tks = mod_tks.new(unicycle=unicycle, exchange=unicycle.get("this", "exchange"), interval=args.interval, verbose=args.verbose)

lores_sample_threshold = unicycle.get_dict("lores_sample_threshold")

default_time = unicycle.get_dbh().get_converted_time(unicycle.get("lores", "1day_default_time"), unicycle.get("lores", "1day_time_zone"), unicycle.get("this", "time_zone"))

intervals = [args.interval] if args.interval != None else hi_tks.get_unicycle().get_dict("lores_sample_threshold").keys()

for table in hi_tks.get_tables(re_str=args.tbl_reg_exp):

    if args.dryrun:
        print table
    else:
        hi_tks.set_table(table=table)
        lo_tks.set_instrument(instrument=hi_tks.get_instrument())

        for interval in intervals:

            lo_tks.set_interval(interval=interval)
            unicycle.get_dbh().drop_table(lo_tks.get_db_tbl())

            if args.verbose:
                print "----------------------------------------------------"
                print "%s %s: %d (sample cnt)" % (interval, table, lo_tks.get_sample_secs() / hi_tks.get_interval_secs())
            else:
                print interval, lo_tks.get_instrument()

            lo_tks.replace_into(hi_db_tbl=hi_tks.get_db_tbl(), hi_interval_secs=hi_tks.get_interval_secs(), threshold=lores_sample_threshold[interval], default_time=default_time)
