#!/bin/bash

for i in $(cat /tmp/symbols_to_delete.txt)
do
  mysql -s -N -e "delete from unicycle.equities_symbols where id = '"$i"';"
  tks=$i"_tks"
  mysql -s -N -e "delete from unicycle.equities_summary where id = '"$tks"';"
  for j in 15sec 1min 15min 1day
  do
    mysql -s -N -e "drop table if exists equities_"$j"."$tks";"
  done
done
