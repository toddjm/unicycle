#!/usr/bin/env python
"""WARNING: tks files must be of the form <symbol>.ext
"""
import mod_tks
import os
import re
import sys
import warnings

files = None
cfg = None
dirname = None
intervals = None

if len(sys.argv) > 2:
    files = sys.argv
    files.pop(0)
    cfg = files.pop(0)

if (files == None): 
    print "%s:[WARNING] No files specified; try: %s <config_filenam> <filenames>" % (sys.argv[0], sys.argv[0])
    sys.exit(0)

hi_tks = mod_tks.new(config_filename=cfg)
lo_tks = mod_tks.new(unicycle=hi_tks.get_unicycle())

lores_sample_threshold = hi_tks.get_unicycle().get_dict("lores_sample_threshold")

index = 1
for fname in files:

    raw_fname = fname
    if (re.search("\.%s$" % hi_tks.get_config().get("feed", "zip_ext"), fname) != None):
        os.system("%s %s" % (hi_tks.get_config().get("feed", "unzip_command"), fname))
        raw_fname = fname[0:(-1 * (len(hi_tks.get_config().get("feed", "zip_ext")) + 1))]

    if not os.path.exists(raw_fname):
        raise StandardError("File does not exist: %s\n" % (raw_fname))
    
    if os.path.getsize(raw_fname) == 0:
        warnings.warn("File is empty... skipping.")
    else:
        hi_tks.set_raw_path(raw_path=raw_fname)

        default_time = hi_tks.get_dbh().get_converted_time(hi_tks.get_config().get("lores", "1day_default_time"),
                                                           hi_tks.get_config().get("lores", "1day_time_zone"),
                                                           hi_tks.get_config().get("feed", "time_zone"))

        print "[%d] %s" % (index, raw_fname)

        hi_tks.replace_into()

        lo_tks.set_db_tbl(db_tbl=hi_tks.get_db_tbl())

        if (intervals == None):
            intervals = hi_tks.get_unicycle().get_dict("interval").keys()
            intervals.remove(hi_tks.get_interval())

        for interval in intervals:
            lo_tks.set_interval(interval=interval)
            lo_tks.replace_into(hi_tks.get_raw_table(),
                                hi_interval_secs=hi_tks.get_interval_secs(),
                                threshold=lores_sample_threshold[interval],
                                default_time=default_time)

        index += 1
