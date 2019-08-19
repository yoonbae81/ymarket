#!/usr/bin/env python3

import argparse
import sys
import re
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
    r = Retry(total=5,
              read=5,
              connect=5,
              backoff_factor=0.2,
              status_forcelist=[500, 502, 504])
    a = HTTPAdapter(max_retries=r)
    s.mount('http://', a)
    s.mount('https://', a)
    return s

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='symbol to fetch prices')
    args = parser.parse_args()

    s = session()
    p = {'page': 1,
         'code': args.symbol,
         'thistime': datetime.now().strftime('%Y%m%d') + '2359'}
    URL = 'https://finance.naver.com/item/sise_time.nhn'

    r = s.get(URL, params=p)
    bs = BeautifulSoup(r.text, 'html.parser')
    pgRR = bs.find('td', class_='pgRR')
    if pgRR is None:
        print('ERROR: No data with {}'.format(args.symbol), file=sys.stderr)
        sys.exit(1)
    last_page = int(re.search(r'page=([0-9]+)', pgRR.a['href']).group(1))

    for page in range(last_page, 0, -1):
        p['page'] = page
        r = s.get(URL, params=p)
        bs = BeautifulSoup(r.text, 'html.parser')

        try:
            for row in parse(args.symbol, bs):
                print('\t'.join(row))
        except IndexError as e:
            print('ERROR: ' + r.request.url, file=sys.stderr)

    print('INFO: {} done'.format(args.symbol), file=sys.stderr)


""" codelet for debug
URL = 'https://finance.naver.com/item/sise_time.nhn'
import easydict
args = easydict.EasyDict({'symbol':'015760'})
p = {'code': args.symbol, 'thistime': '201908192359', 'page': 1}
r = requests.get(URL, params=p)
bs = BeautifulSoup(r.text, 'html.parser')
bs.find('td', class_='pgRR') == None
"""
