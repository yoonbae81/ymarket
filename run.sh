#!/bin/bash

#date=$(date '+%Y-%m-%d')
date="2019-08-16"
temp="/tmp/day"
output="$HOME/yQuant.data/day/$date.txt"

echo "Fetching daily prices..."
rm -rf $temp
~/market/day_all.py -o $temp

cat $temp | grep $date | awk 'BEGIN {FS=","}; {OFS="\t"}; {print $2,$3,$4,$5,$6,$7}' | sort > $output
lines=`cat $output | wc -l`
echo "$lines prices wrote to $output"
