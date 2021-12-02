#!/bin/bash
DATE=${1:-`date +%Y-%m-%d`}
DIR=$HOME/market

######################
# Symbols
######################
SYMBOLS=$(mktemp /tmp/symbols-kr.XXXX)
$DIR/symbol-kr.py > $SYMBOLS
wc -l $SYMBOLS | ~/bin/telegram - > /dev/null

######################
# Day
######################
TEMP=$(mktemp /tmp/day-kr.XXXX)
cat $SYMBOLS | parallel --jobs 6 -N100 --pipe $DIR/day-kr.py -d $DATE -f - > $TEMP
mkdir -p $DIR/data/day-kr
sort $TEMP > $DIR/data/day-kr/$DATE.txt
wc -l $DIR/data/day/$DATE.txt | ~/bin/telegram - > /dev/null

######################
# Minute
######################
TEMP=$(mktemp /tmp/minute.XXXX)
cat $SYMBOLS | parallel --jobs 6 -N100 --pipe $DIR/minute-kr.py -d $DATE -f - > $TEMP
mkdir -p $DIR/data/minute-kr
sort -k4 $TEMP > $DIR/data/minute-kr/$DATE.txt
wc -l $DIR/data/minute/$DATE.txt | ~/bin/telegram - > /dev/null
