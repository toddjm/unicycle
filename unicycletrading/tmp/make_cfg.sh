#!/bin/bash

file=$1

base=${file%%"."*}

cat $file | awk 'FS="\t" {print $1}' | sed 's/\//\_/g' | sed 's/\./\_/g' | sort > $base.temp

cat $base.temp | awk '{print "\tequities_1day."$1"_tks\t1=1"}' > $base.temp.cfg

cat header.cfg $base.temp.cfg > $base.cfg

rm -f $base.temp $base.temp.cfg
