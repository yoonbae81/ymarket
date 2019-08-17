#!/usr/bin/python3

# simply run
# $ ./day.py

# $ scrapy runspider --nolog -t csv -o - -a symbol=015760 day.py

import re
import argparse
import scrapy
from scrapy.exporters import CsvItemExporter
from scrapy.crawler import CrawlerProcess

RE = re.compile("code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn")

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
         # 'http://xcv.kr/symbol.html'
       'http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S',
       'http://finance.daum.net/xml/xmlallpanel.daum?stype=Q&type=S'
    ]

    # def __init__(self, symbol):
    #     self.symbol = symbol

    # def start_requests(self):
    #     url = URL.format(self.symbol)
    #     yield scrapy.Request(url,
    #                          callback=self.parse,
    #                          meta={'symbol': self.symbol})

    def parse(self, response):
        for m in re.finditer(RE, response.text):
            symbol = m.group(1)
            url = 'https://finance.naver.com/item/sise_day.nhn?code={}'.format(symbol)

            yield scrapy.Request(url, callback=self.parse2, meta={'symbol': symbol})

    def parse2(self, response):
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
    parser.add_argument('-o', '--output', help='directory to save output')
    args = parser.parse_args()

    process = CrawlerProcess(settings={
        'FEED_URI': 'stdout:' if args.output is None else args.output,
        'FEED_FORMAT': 'csv',
        'LOG_ENABLED': True
    })

    process.crawl(Spider)
    process.start()


''' codelet for scrapy shell
symbol = '015760'
url = 'https://finance.naver.com/item/sise_day.nhn?code={}'.format(symbol)
fetch(url)
'''
