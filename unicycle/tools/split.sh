#!/bin/bash

for i in `cat fx.txt.original | awk '{print $1}'`
    do echo -e "$i\tIDEALPRO\t07:00:00\t14:00:00\t${i:3}"
done
