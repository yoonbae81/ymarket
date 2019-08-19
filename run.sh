#!/bin/bash

DATE=$(date '+%Y-%m-%d')
#DATE="2019-08-16"
BASE=$HOME/market

######################
# Symbols
######################
SYMBOLS=$(mktemp)
$BASE/symbol.py | tail -n +2 | awk 'BEGIN {FS=","}; {print $1}' > $SYMBOLS
echo "Symbols: `cat $SYMBOLS | wc -l`"

######################
# Day
######################
# TEMP=$(mktemp)
# $BASE/day.py -l $SYMBOLS -o $TEMP
# DIR=$BASE/data.day
# mkdir -p $DIR
# cat $TEMP | grep $DATE | awk 'BEGIN {FS=","}; {OFS="\t"}; {print $2,$3,$4,$5,$6,$7}' | sort > $DIR/$DATE.txt
# echo `wc -l $DIR/$DATE.txt`

######################
# Minute
######################
TEMP=$(mktemp)
cat $SYMBOLS | parallel --jobs 20 --group $BASE/minute.py {} > $TEMP
DIR=$BASE/data.minute
mkdir -p $DIR
sort -k4 $TEMP > $DIR/$DATE.txt
echo `wc -l $DIR/$DATE.txt`
