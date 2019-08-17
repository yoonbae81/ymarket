#!/bin/bash

#DATE=$(date '+%Y-%m-%d')
DATE="2019-08-16"
TEMP="/tmp/market"
DIR="$HOME/yQuant.data"

echo "Fetching symbols list"
SYMBOLS="$TEMP/symbols"
~/market/symbol.py | tail -n +2 | awk 'BEGIN {FS=","}; {print $1}' > $SYMBOLS

# output="$HOME/yQuant.data/day/$date.txt"
#
# echo "Fetching daily prices..."
# rm -rf $temp
# ~/market/day_all.py -o $temp
#
# cat $temp | grep $date | awk 'BEGIN {FS=","}; {OFS="\t"}; {print $2,$3,$4,$5,$6,$7}' | sort > $output
# lines=`cat $output | wc -l`
# echo "$lines prices wrote to $output"
