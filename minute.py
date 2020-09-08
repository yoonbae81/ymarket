#!/usr/bin/env python3

import argparse
import sys
import re
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

URL = 'https://finance.naver.com/item/sise_time.nhn'

def partition(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def parse(symbol, bs):
    values = [span.text for span in bs.findAll('span', class_='tah')]
    values = list(map(lambda s: s.strip().replace(',', ''), values))
    result = []
    for row in partition(values, 7):
        result.insert(0, [symbol, row[1], row[6], row[0]])
    return result

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
        p = {'page': 1,
             'code': symbol,
             'thistime': date.replace('-', '') + '2359'}
        r = s.get(URL, params=p)
        bs = BeautifulSoup(r.text, 'html.parser')

        pgRR = bs.find('td', class_='pgRR')
        if pgRR is None:
            print('[ERROR] Invalid symbol: {}'.format(symbol), file=sys.stderr)
            continue
        last_page = int(re.search(r'page=([0-9]+)', pgRR.a['href']).group(1))

        for page in range(last_page, 0, -1):
            p['page'] = page
            r = s.get(URL, params=p)
            bs = BeautifulSoup(r.text, 'html.parser')

            for row in parse(symbol, bs):
                print('\t'.join(row))

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
URL = 'https://finance.naver.com/item/sise_time.nhn'
import easydict
args = easydict.EasyDict({'symbol':'015760'})
p = {'code': args.symbol, 'thistime': '201908192359', 'page': 1}
r = requests.get(URL, params=p)
bs = BeautifulSoup(r.text, 'html.parser')
bs.find('td', class_='pgRR') == None
"""
