#!/bin/bash
#************************************#
#       push_ticks.sh                #
#                                    #
# Given directory, will tar and gzip #
# that directory and ship it to the  #
# cloud. The .ref file in the source #
# tree is touched and added if not   #
# present, then committed.           #
#************************************#

ARGS=1

if [ $# -ne 1 ]
then
    echo "Usage: ./push_ticks.sh YYYYMMDD"
    exit
fi

target_dir=$1  # Target directory.

if [ ! -d "$target_dir" ]  # Test if directory exists.
then
    echo "$target_dir does not exist."
    exit
fi

ticks_dir=${PWD##"$HOME/unicycletrading/"}    # Set this to pwd, remove /Users/username/unicycle/.
ref_file=$ticks_dir/$target_dir.ref # Set the path and name of the ref_file.

tar cvf $target_dir.tar $target_dir
gzip $target_dir.tar

cd $HOME/unicycletrading

gsutil cp $ticks_dir/$target_dir.tar.gz gs://unicycletrading  # Copy .tar.gz file to the cloud.

if [ ! -f "$ref_file" ]  # Test to see if .ref exists.
then
    touch $ref_file
    svn add $ref_file
fi

date > $ref_file  # Cat system date to ref_file.
svn commit $ref_file -m "new"  # Commit ref_file.

mv $ticks_dir/$target_dir.tar.gz $ticks_dir/gz_files

