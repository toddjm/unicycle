#!/bin/bash
#***************************************#
#    roll.sh                            #
#                                       #
# Show volume traded for all symbols    #
# specified for a given month and year. #
#                                       #
#***************************************#

ARGS=2

if [ $# -ne "$ARGS" ]
then
    echo "Usage: `basename $0` YYYYMM symbol"
    exit
fi

datestr=$1    # YYYYMM date format.
symbol=$2     # Symbol name.

for dir in `ls -d "$datestr"[0-9][0-9]`
do echo "$dir:" && for fn in `ls $dir/$symbol[0-9]*`
    do if [ "${fn##*.}" == "bz2" ]
	    then
	    echo -ne "\t" `basename ${fn%%.*}` && \
	    echo -ne " " && \
	    bzcat $fn | awk 'NF != 8 {sum += $7} END {print sum}'
	elif [ "${fn##*.}" == "ticks" ]
	    then
	    echo -ne "\t" `basename ${fn%%.*}` && \
	    echo -ne " " && \
	    awk 'NF != 8 {sum += $7} END {print sum}' < $fn
	else
	    echo "ERROR: Unknown file extension"
	    exit
	fi
    done
done
