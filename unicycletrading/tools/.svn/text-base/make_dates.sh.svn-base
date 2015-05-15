#!/bin/bash
# Copyright bicycle trading, llc.

if [[ $# != 2 ]]
then
    echo "Usage: $0 start_date end_date"
    exit
fi

current_date=`date +"%Y%m%d" -d "$1"`
end_date=`date +"%Y%m%d" -d "$2"`

holidays_list_path="$UNICYCLE_HOME/share/dates"

if [[ ! -e "$holidays_list_path/holidays.txt" ]]
then
    echo "holidays.txt not found"
    exit
fi

holidays_list="$holidays_list_path/holidays.txt"

while [[ $current_date != $end_date ]]
do
    day_of_week=`date -d "$current_date" +%A`
    if [[ $day_of_week != Saturday ]] && \
       [[ $day_of_week != Sunday ]] && \
       [[ ! `grep $current_date $holidays_list` ]]
    then
#        echo "mkdir -p ${current_date:0:4}/${current_date:4:2}/${current_date:6:2}"
#        echo "start time: `date +%s -ud $current_date`"
        echo $current_date
    fi
    current_date=`date +"%Y%m%d" -d "$current_date + 1 day"`
done
