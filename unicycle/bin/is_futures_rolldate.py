#!/usr/bin/env python

import argparse
import mod_futures
import mod_tks
import mod_unicycle
import re

class My_ArgumentParser(argparse.ArgumentParser):
    def get_usage_str(self, print_body=False):
        out = ""
        repl_str = "\[-instrument.*\]\]"
        body = super(My_ArgumentParser, self).format_help().split('\n')
        header = body.pop(0)
        fld = re.findall(" (%s)" % repl_str, header).pop()
        header = re.sub(" %s" % (repl_str), '', header)
        out += "%s %s\n" % (header, fld)
        if print_body:
            out += "%s" % ('\n'.join(body))
        return out

    def print_help(self):
        print self.get_usage_str(print_body=True)

    def print_usage(self, file):
        file.write(self.get_usage_str())

parser = My_ArgumentParser(description='Display auto and manual roll dates.')
parser.add_argument('config_filename',
                    help='the configuration file for the asset class')
parser.add_argument('from_date',
                    nargs='?',
                    default=None,
                    help='from date, e.g. 20100101 (default: None)')
parser.add_argument('to_date',
                    nargs='?',
                    default=None,
                    help='to date, e.g. 20110211 (default: None)')
parser.add_argument('-instrument',
                    nargs='+',
                    help='valid futures instruments, e.g. CL')
args = parser.parse_args()

found = {}
man_found = {}
contract_cnt = 2
unicycle = mod_unicycle.new(config_filename=args.config_filename,
                            autoset_time_zone=False)
tks = mod_tks.new(unicycle=unicycle,
                  exchange=unicycle.get("this", "exchange"),
                  interval="1day")

fm = mod_futures.new()

if ((args.from_date != None) and (args.to_date != None)):
    dates = tks.get_dbh().get_list(
      "SELECT DATE_FORMAT(ts, '%s') FROM %s.%s "
      "WHERE DATE(ts) >= '%s' and DATE(ts) < '%s' ORDER BY ts" % (
      "%Y%m%d",
      unicycle.get("mysql", "default_db"),
      mod_unicycle.get_valid_table(unicycle.get("this", "exchange"), "1day"),
      args.from_date,
      args.to_date))
else:
    dates = [tks.get_dbh().get_date_str()]

for date_str in dates:

    print "Date = %s" % date_str

    days = tks.get_dbh().get_list(
      "SELECT DATE_FORMAT(ts, '%s') FROM %s.%s WHERE "
      "DATE(ts) <= '%s' ORDER BY ts DESC LIMIT 3" % (
      "%Y%m%d",
      unicycle.get("mysql", "default_db"),
      mod_unicycle.get_valid_table(unicycle.get("this", "exchange"), "1day"),
      date_str))

    fm.set_from_date(days[1])

    if (args.instrument == None):
        instruments = tks.get_dbh().get_list(
          "SELECT id FROM %s.futures ORDER "
          "BY id" % unicycle.get("mysql", "default_db"))
    else:
        instruments = args.instrument

    for instrument in instruments: 

        fm.set_instrument(instrument)
        contract_months = fm.get_unadjusted_relative_contract_months()[0:contract_cnt]

        if (not found.has_key(instrument)):
            found[instrument] = {}
            man_found[instrument] = {}

        roll_date = None
        if (len(contract_months)):
            roll_date = tks.get_dbh().get_one(
              "SELECT DATE_FORMAT(roll_date, '%s') FROM "
              "%s.futures_roll_dates WHERE id='%s' AND "
              "IFNULL(contract, DATE_FORMAT(last_trade_date, "
              "'%s'))='%s'" % (
              "%Y%m%d",
              unicycle.get("mysql", "default_db"),
              instrument,
              "%Y%m",
              contract_months[0]))
            if (roll_date == days[1]):
                man_found[instrument][contract_months[0]] = True
                print "MANUAL ROLL [%s%s = %s]:" % (
                  instrument,
                  contract_months[0],
                  days[1]),

        valid_cnt = 0
        volume = {}
        mavg = {}

        for contract_month in contract_months:
            tks.set_table(table="%s%s_tks" % (instrument, contract_month))

            if tks.get_dbh().table_exists(tks.get_table()):
                volume[contract_month] = {}
                vol_sum = 0
                for day in days:
                    volume[contract_month][day] = tks.get_dbh().get_one(
                      "SELECT volume FROM %s WHERE DATE(ts)='%s'" % (
                      tks.get_table(),
                      day))
                    if (volume[contract_month][day] != None):
                        vol_sum += volume[contract_month][day]
                        valid_cnt += 1
                mavg[contract_month] = float(vol_sum) / len(days)
                    
        if ((len(contract_months) == contract_cnt) and
            (not found[instrument].has_key(contract_months[0])) and
            (valid_cnt == (len(days) * contract_cnt))):
            
            if ((volume[contract_months[1]][days[0]] > volume[contract_months[0]][days[0]]) and
                (volume[contract_months[0]][days[2]] > volume[contract_months[0]][days[0]]) and
                (volume[contract_months[1]][days[0]] > volume[contract_months[1]][days[2]]) and
                (float(volume[contract_months[0]][days[0]]) < mavg[contract_months[0]]) and
                (float(volume[contract_months[1]][days[0]]) > mavg[contract_months[1]])):

                found[instrument][contract_months[0]] = True
                if ((roll_date != None) and (roll_date != days[1])):
                    print "DIFF ",

                print "AUTO ROLL: [%s%s = %s] ->" % (
                  instrument,
                  contract_months[0],
                  days[1]),
                print volume
            else:
                print
        else:
            print "[%s = %s] ->" % (instrument, days[1]),
            print volume

for instrument in man_found.keys():
    for contract in man_found[instrument].keys():
        if (not found[instrument].has_key(contract)):
            print "AUTO ROLL NOT FOUND: [%s%s]" % (instrument, contract)
