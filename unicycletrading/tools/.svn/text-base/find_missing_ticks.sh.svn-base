#!/bin/bash
#***********************************************
#     find_missing_ticks.sh                     
#                                               
# Given a list of symbols.ticks, will copy      
# these files and unicycle.cfg from the source  
# directory to a new directory with name        
# YYYYMMDDx. Note that this directory must      
# already exist, as well as file "list."        
#***********************************************

ARGS=2

if [ $# -ne "$ARGS" ]
then
    echo "Usage: ./find_missing_ticks.sh YYYY MM"
    exit
fi

YYYY=$1  # Year.
MM=$2    # Month.

for i in `ls -d $YYYY$MM[0-9][0-9]`
do
    if [ -d "$i" ]
    then
	for j in `cat list`
	do
	    if [ -e "$i/$j" ]
	    then
		cp "$i/$j" `basename $i`c && \
		cp "$i"/unicycle.cfg `basename $i`c
	    fi
	done
    fi
done
