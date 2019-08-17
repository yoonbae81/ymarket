#!/usr/bin/python3

import re
import scrapy
from scrapy.exporters import CsvItemExporter
from scrapy.crawler import CrawlerProcess

RE = re.compile("code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn")

class RawExporter(CsvItemExporter):
    def __init__(self, *args, **kwargs):
        kwargs['include_headers_line'] = False
        kwargs['join_multivalued'] = '_'

        super(RawExporter, self).__init__(*args, **kwargs)

class Pipeline(object):
    def process_item(self, item, spider):
        if 'price' in item:
            item['price'] = item['price'].replace(",", "")
        return item

class Spider(scrapy.Spider):
    name = "symbol"

    custom_settings = {
        # 'FEED_URI': 'stdout:',
        # 'FEED_EXPORTERS': { 'csv': 'symbol.RawExporter' },
        # 'ITEM_PIPELINES': { 'symbol.Pipeline': 0},
        'FEED_EXPORT_ENCODING': 'utf-8',
        'CONCURRENT_REQUESTS': 2
    }

    start_urls = [
        'http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S',
        'http://finance.daum.net/xml/xmlallpanel.daum?stype=Q&type=S'
    ]

    def parse(this, response):
        for line in re.finditer(RE, response.text):
            yield {
                'symbol': line.group(1),
                'name': line.group(2),
                'price': line.group(3).replace(",", "")
            }

if __name__ == '__main__':
    process = CrawlerProcess(settings={
        'FEED_URI': 'stdout:',
        'FEED_FORMAT': 'csv',
        'LOG_ENABLED': False
    })

    process.crawl(Spider)
    process.start()

    # equivalent above
    # scrapy runspider symbol.py --nolog -o - -t csv




''' debug code
import requests
response = requests.get("http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S")

for line in re.finditer(RE, response.text):
    print({'symbol': line.group(1), 'name': line.group(2)})
'''
