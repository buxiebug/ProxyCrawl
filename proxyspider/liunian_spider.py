# encoding:utf8
__author__ = 'brianyang'

import re

from proxyspider.spider import Spider


class LiunianSpider(Spider):
    def __init__(self, count=10):
        super(Spider, self).__init__()
        self.count = count

    def crawl_urls(self):
        return ['http://www.89ip.cn/tiqu.php?sxb=&tqsl={}&ports=&ktip=&xl=on&submit=%CC%E1++%C8%A1'.format(self.count)]

    def crawl_callback(self, text):
        proxies = []
        results = re.findall("((?:\d{1,3}.){3}\d{1,3}:\d{2,4})", text)
        for result in results:
            proxies.append(result)
        return proxies
