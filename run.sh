#!/bin/bash

DATE=$(date '+%Y-%m-%d')
#DATE="2019-08-16"
TEMP="/tmp/market"
DIR="$HOME/market/data"

rm -rf $TEMP
mkdir $TEMP

SYMBOLS="$TEMP/symbols"
~/market/symbol.py | tail -n +2 | awk 'BEGIN {FS=","}; {print $1}' > $SYMBOLS
COUNT=`cat $SYMBOLS | wc -l`
echo "Symbols: $COUNT"

OUTPUT="$DIR/day/$DATE.txt"
~/market/day.py -l $SYMBOLS -o $TEMP/day
cat $TEMP/day | grep $DATE | awk 'BEGIN {FS=","}; {OFS="\t"}; {print $2,$3,$4,$5,$6,$7}' | sort > $OUTPUT
COUNT=`cat $OUTPUT | wc -l`
echo "Day: $COUNT"

OUTPUT="$DIR/minute/$DATE.txt"
~/market/minute.py -l $SYMBOLS -o $TEMP/minute
cat $TEMP/minute | tail -n +2 | awk 'BEGIN {FS=","}; {OFS="\t"}; {print $1,$2,$3,$4}' | sort -n -s -k 4 > $OUTPUT
COUNT=`cat $OUTPUT | wc -l`
echo "Minute: $COUNT"
