#!/usr/bin/env python3

import argparse
import sys
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

URL = 'https://finance.naver.com/item/sise_day.nhn'

def partition(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def parse(bs):
    fields = ['date', 'close', 'delta', 'open', 'high', 'low', 'volume']
    values = [span.text for span in bs.findAll('span', class_='tah')]
    values = list(map(lambda s: s.strip().replace(',', '').replace('.', '-'), values))
    for row in partition(values, 7):
       yield dict(zip(fields, row))

def session():
    s = requests.session()
    r = Retry(total=10,
              read=5,
              connect=5,
              backoff_factor=1,
              status_forcelist=[500, 502, 504])
    a = HTTPAdapter(max_retries=r)
    s.mount('http://', a)
    s.mount('https://', a)
    return s

def main(date, symbols):
    s = session()
    for symbol in symbols:
        r = s.get(URL, params={'code': symbol, 'page': 1})
        bs = BeautifulSoup(r.text, 'html.parser')

        if bs.find('td', class_='pgRR') is None:
            print('[ERROR] Invalid symbol: {}'.format(symbol), file=sys.stderr)
            continue

        for row in parse(bs):
            if row['date'] == args.date and row['open'] != '0':
                print('\t'.join([symbol, row['open'], row['high'], row['low'], row['close'], row['volume']]))
                break

        print('[INFO] {} done'.format(symbol), file=sys.stderr)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', default=datetime.now().strftime('%Y-%m-%d'), help='format: YYYY-MM-DD')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--file', type=argparse.FileType('r'), help='symbol file')
    group.add_argument('-s', '--symbol')
    args = parser.parse_args()

    symbols = [args.symbol] if args.symbol else [line.rstrip() for line in args.file]
    main(args.date, symbols)

""" codelet for debug
import easydict
args = easydict.EasyDict({'date': '2019-08-24', 'symbol': 'AAA'})
p = {'date': args.date, 'code': args.symbol}
r = requests.get(URL, params=p)
bs = BeautifulSoup(r.text, 'html.parser')
next(parse(bs))
"""
