#!/bin/bash

#date=$(date '+%Y-%m-%d')
date="2019-08-16"
symbol="/tmp/symbol"
temp="/tmp/day"
output="~/$date.txt"

# ~/market/symbol.py | tail -n +2 | awk 'BEGIN {FS=","}; {print $1}' > $symbol
cat $symbol |head | parallel -j 400% ~/market/day.py {} -s $symbol -o $temp
cat $temp | grep $date | awk 'BEGIN {FS=","}; {OFS="\t"}; {print $2,$3,$4,$5,$6,$7}' | sort > $output

