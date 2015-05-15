#!/usr/bin/env python
"""
"""
import mod_tks

tks = mod_tks.new(asset="equities", interval="1day")

#for table in tks.get_tables():
#    row = tks.get_dbh().get_row("SELECT AVG(volume),"
#                                "STD(volume),"
#                                "COUNT(volume) FROM {0}".format(table))
#    if row[0] == None:
#        row = [0, 0, 0]
#    print '{0} {1:.1f} {2:.1f} {3:3d}'.format(table[0:-4], row[0] * 100,
#                                              row[1] * 100, row[2])

for table in tks.get_tables():
    row = tks.get_dbh().get_row("SELECT AVG(volume),"
                                "COUNT(volume) FROM {0}".format(table))
    if row[0] == None:
        row = [0, 0]
    print '{0} {1:.0f} {2:3d}'.format(table[0:-4], row[0] * 100, row[1])
