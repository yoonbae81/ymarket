#!/usr/bin/python3

# $ ./minute.py -s 015760
# $ ./minute.py -l symbols.txt -o output.txt

# run with scrapy shell that supports various format
# $ scrapy runspider --nolog -t csv -o -  minute.py
# $ scrapy runspider --nolog -t -o -  minute.py

import re
import argparse
import scrapy
from datetime import datetime
from scrapy.exporters import CsvItemExporter
from scrapy.crawler import CrawlerProcess

DATE = datetime.now().strftime("%Y%m%d")
#DATE = '20190816'
URL = 'https://finance.naver.com/item/sise_time.nhn?thistime={}2359&code={}&page={}'

def partition(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

class Spider(scrapy.Spider):
    name = "day"

    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'CONCURRENT_REQUESTS': 20,
        'TELNETCONSOLE_ENABLED': False
    }

    def __init__(self, symbols):
         self.symbols = symbols

    def start_requests(self):
        for symbol in self.symbols:
            yield scrapy.Request(URL.format(DATE, symbol, 1),
                                 callback=self.parse,
                                 meta={'symbol': symbol, 'page': 1})

    def parse(self, response):
        symbol = response.meta['symbol']
        page = int(response.meta['page'])

        r = response.css('span.tah::text').getall()
        m = map(lambda s: s.replace(',', '').replace('\t', '').replace('\n', '').replace('.', '-'), r)
        for e in partition(list(m), 7):
            yield {
                'symbol': symbol,
                'price': e[1],
                'volume': e[6],
                'time': e[0]
#                'timestamp': int(datetime.strptime('{} {}'.format(DATE, e[0]), '%Y%m%d %H:%M').timestamp())
            }

        last_page = int(re.search('page=(\d.)', response.css('td.pgRR').css('a').get()).group(1))
        if (page < last_page):
            url = URL.format(DATE, symbol, page+1)
            yield scrapy.Request(url,
                                 callback=self.parse,
                                 meta={'symbol': symbol, 'page': page+1})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--symbol', help='a symbol to fetch')
    parser.add_argument('-l', '--list', help='a file of symbols list')
    parser.add_argument('-o', '--output', help='directory to save output')
    args = parser.parse_args()

    process = CrawlerProcess(settings={
        'FEED_URI': 'stdout:' if args.output is None else args.output,
        'FEED_FORMAT': 'csv',
        'LOG_ENABLED': False
    })

    if args.symbol:
        symbols = [args.symbol]
    else:
        with open(args.list) as f:
            symbols = f.read().splitlines()

    process.crawl(Spider, symbols)
    process.start()
