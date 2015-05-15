#!/usr/bin/env python
"""tks2cloud

"""


import mod_unicycle
import os
import sys

unicycle = mod_unicycle.new(config_filename=sys.argv[1])

# S20110210_02.rtks
tks = os.path.basename(sys.argv[2])

# S20110210_02
base = os.path.splitext(tks)[0]

row = unicycle.get_dbh().get_row("SELECT DATE_FORMAT(NOW(), "
                                 "'%Y%m%d'), DATE_FORMAT(NOW(), '%H')")
now_base = "S%s_%s" % (row[0], row[1])

#if not (now_base == base):
if now_base != base:

    # $TICKS_HOME/equities/ib/15sec
    path = "%s/%s/%s/%s" % (os.getenv("TICKS_HOME"),
                            unicycle.get("exchange_dir",
                                         unicycle.get("this",
                                                      "exchange")),
                            "ib", unicycle.get("hires", "interval"))

    # chdir to path
    os.chdir(path)

    # ['S20110210', '02']
    dirs = base.split("_")

    # S20110210_02.tar.gz
    tarball = "%s.tar.gz" % base

    ref = "%s.ref" % base
    utks = "%s.utks" % base

    # $ARCHIVE_HOME/equities/ib/15sec
    archive_path = "%s/%s/%s/%s" % (os.getenv("ARCHIVE_HOME"),
                                    unicycle.get("exchange_dir",
                                                 unicycle.get("this",
                                                              "exchange")),
                                    "ib", unicycle.get("hires", "interval"))

    # create tarball
    # tar zcf S20110210_02.tar.gz S20110210/02/
    os.system("tar zcf %s %s/%s/" % (tarball, dirs[0], dirs[1]))

    # copy tarball to archive
    # cp S20110210_02.tar.gz $ARCHIVE_HOME/equities/ib/15sec/
    os.system("cp %s %s/%s" % (tarball, archive_path, tarball))

    # create ref as tar listing
    # tar ztf S20110210_02.tar.gz > S20110210_02.ref
    os.system("tar ztf %s > %s" % (tarball, ref))

    # rm tarball
    os.system("rm -f %s" % tarball)

    # svn
    os.system("svn add %s" % ref)
    os.system("svn commit %s -m 'new'" % ref)

    # create utks
    os.system("touch %s" % utks)
