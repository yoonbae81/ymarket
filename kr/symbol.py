#!/usr/bin/env python3

import json
import requests
import sys

URLS = ['https://finance.daum.net/api/quotes/stocks?market=KOSPI',
        'https://finance.daum.net/api/quotes/stocks?market=KOSDAQ']

HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
    'referer': 'https://finance.daum.net/domestic/all_quotes',
}


def parse(res):
    data = json.loads(res.text)['data']
    for item in data:
        item['symbol'] = item.pop('code')[3:9]
        item['price'] = item.pop('tradePrice')
        del item['symbolCode']
        del item['changePrice']
        del item['changeRate']
        del item['change']

        yield item


if __name__ == '__main__':
    try:
        for url in URLS:
            res = requests.get(url, headers=HEADERS)

            if res.status_code != 200:
                print('ERROR: HTTP Status Code {}'.format(res.status_code), file=sys.stderr)
                sys.exit(1)

            for item in parse(res):
                print('{symbol}'.format(**item))

    except Exception as e:
        print(e, file=sys.stderr)

""" Codelet for debug
res = requests.get(URLS[0], headers=HEADERS)
res.status_code == 200
data = json.loads(res.text)['data']
item = data[0]
"""
