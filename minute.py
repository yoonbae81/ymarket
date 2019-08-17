#!/usr/bin/python3

# simply run
# $ ./minute.py

# run with scrapy shell that supports various format
# $ scrapy runspider --nolog -t csv -o -  minute.py
# $ scrapy runspider --nolog -t json -o -  minute.py

import re
import argparse
import scrapy
from datetime import datetime
from scrapy.exporters import CsvItemExporter
from scrapy.crawler import CrawlerProcess

# DATE = datetime.now().strftime("%Y%m%d")
DATE = '20190816'
RE = re.compile("code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn")
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

    start_urls = [
       'http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S',
       'http://finance.daum.net/xml/xmlallpanel.daum?stype=Q&type=S'
    ]

    # def __init__(self, symbol):
    #     self.symbol = symbol
    #     self.page = 1
    #
    # def start_requests(self):
    #     url = URL.format(DATE, self.symbol, self.page)
    #     yield scrapy.Request(url,
    #                          callback=self.parse,
    #                          meta={'symbol': self.symbol,
    #                                'page': self.page})

    def parse(self, response):
        for m in re.finditer(RE, response.text):
            symbol = m.group(1)
            page = 1
            url = URL.format(DATE, symbol, page)
            yield scrapy.Request(url,
                                 callback=self.parse2,
                                 meta={'symbol': symbol, 'page': page})

    def parse2(self, response):
        symbol = response.meta['symbol']
        page = int(response.meta['page'])

        r = response.css('span.tah::text').getall()
        m = map(lambda s: s.replace(',', '').replace('\t', '').replace('\n', '').replace('.', '-'), r)
        for e in partition(list(m), 7):
            yield {
                'symbol': symbol,
                'price': e[1],
                'volume': e[5],
                'timestamp': int(datetime.strptime('{} {}'.format(DATE, e[0]), '%Y%m%d %H:%M').timestamp())
            }

        last_page = int(re.search('page=(\d.)', response.css('td.pgRR').css('a').get()).group(1))
        if (page < last_page):
            url = URL.format(DATE, symbol, page+1)
            yield scrapy.Request(url,
                                 callback=self.parse2,
                                 meta={'symbol': symbol, 'page': page+1})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('symbol', help='directory to save output')
    parser.add_argument('-o', '--output', help='directory to save output')
    args = parser.parse_args()

    process = CrawlerProcess(settings={
        # 'FEED_URI': 'stdout:',
        'FEED_URI': 'stdout:' if args.output is None else args.output,
        'FEED_FORMAT': 'csv',
        'LOG_ENABLED': False
    })

    # process.crawl(Spider, args.symbol)
    process.crawl(Spider)
    process.start()


''' codelet for scrapy shell
symbol = '015760'
url = 'https://finance.naver.com/item/sise_day.nhn?code={}'.format(symbol)
fetch(url)
'''
