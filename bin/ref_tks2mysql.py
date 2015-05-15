#!/usr/bin/env python
"""ref_tks2mysql

"""

import mod_unicycle
import os
import sys


refs = sys.argv
refs.pop(0)
cfg = refs.pop(0)
unicycle = mod_unicycle.new(config_filename=cfg)
tks2mysql = "%s/bin/tks2mysql.py" % os.getenv("UNICYCLE_HOME")

for ref in refs:
    # set path to ref: $TICKS_HOME/futures/ib/15sec/20100802.ref
    path = "%s/%s/%s/%s" % (os.getenv("TICKS_HOME"),
                            unicycle.get("exchange_dir",
                                         unicycle.get("this",
                                                      "exchange")),
                            "ib", unicycle.get("hires", "interval"))

    (base, ext) = os.path.splitext(os.path.basename(ref))

    print "%s/%s%s" % (path, base, ext)

    # make tmpdir
    tmpdir = "/tmp/%s_%s" % (unicycle.get("this", "exchange"), base)
    if (os.path.exists(tmpdir)):
        os.system("rm -rf %s" % tmpdir)
    os.system("mkdir %s" % tmpdir)

    # get tarball
    tarball = "/tmp/%s.tar.gz" % base

    # ref: $ARCHIVE_HOME/futures/ib/15sec/20100802.tar.gz
    archive_path = "%s/%s/%s/%s" % (os.getenv("ARCHIVE_HOME"),
                                    unicycle.get("exchange_dir",
                                                 unicycle.get("this",
                                                              "exchange")),
                                    "ib", unicycle.get("hires",
                                                       "interval"))

    # copy <ref>.tar.gz from archive
    os.system("cp %s/%s.tar.gz %s" % (archive_path, base, tarball))

    # untar
    os.system("tar zxf %s -C %s" % (tarball, tmpdir))

    # tks2mysql
    if not os.system("find %s -name \"*.tks*\" | xargs %s %s" %
            (tmpdir, tks2mysql, cfg)) == 0:
        raise StandardError("Fatal Error: %s" % tks2mysql)

    # cleanup
    os.system("rm -rf %s" % tarball)
    os.system("rm -rf %s" % tmpdir)
    os.system("rm -rf %s/%s.tar.gz" % (path, base))
