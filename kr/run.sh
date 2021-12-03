#!/bin/bash
DATE=${1:-`date +%Y-%m-%d`}
DIR=$HOME/market/kr

######################
# Symbols
######################
SYMBOLS=$(mktemp /tmp/symbols.XXXX)
$DIR/symbol.py > $SYMBOLS
wc -l $SYMBOLS | ~/bin/telegram - > /dev/null

######################
# Day
######################
TEMP=$(mktemp /tmp/day.XXXX)
cat $SYMBOLS | parallel --jobs 6 -N100 --pipe $DIR/day.py -d $DATE -f - > $TEMP
mkdir -p $DIR/day
sort $TEMP > $DIR/day/$DATE.txt
wc -l $DIR/day/$DATE.txt | ~/bin/telegram - > /dev/null

######################
# Minute
######################
TEMP=$(mktemp /tmp/minute.XXXX)
cat $SYMBOLS | parallel --jobs 6 -N100 --pipe $DIR/minute.py -d $DATE -f - > $TEMP
mkdir -p $DIR/minute
sort -k4 $TEMP > $DIR/minute/$DATE.txt
wc -l $DIR/minute/$DATE.txt | ~/bin/telegram - > /dev/null
