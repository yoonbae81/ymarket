#!/usr/bin/python3

# simply run
# $ ./day.py -s 015760
# $ ./day.py -l symbols.txt -o output.txt

# $ scrapy runspider --nolog -t csv -o - -a symbol=015760 day.py

import argparse
import scrapy
from scrapy.exporters import CsvItemExporter
from scrapy.crawler import CrawlerProcess

URL = 'https://finance.naver.com/item/sise_day.nhn?code={}'

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
            yield scrapy.Request(URL.format(symbol),
                                 callback=self.parse,
                                 meta={'symbol': symbol})

    def parse(self, response):
        r = response.css('span.tah::text').getall()
        m = map(lambda s: s.replace(',', '').replace('\t', '').replace('\n', '').replace('.', '-'), r)
        for e in partition(list(m), 7):
            yield {
                'date': e[0],
                'symbol': response.meta['symbol'],
                'open': e[3],
                'high': e[4],
                'low': e[5],
                'close': e[1],
                'volume': e[6]
            }

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


''' codelet for scrapy shell
symbol = '015760'
url = 'https://finance.naver.com/item/sise_day.nhn?code={}'.format(symbol)
fetch(url)
'''
