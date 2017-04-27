# encoding:utf8
__author__ = 'brianyang'

from bs4 import BeautifulSoup as bs

from proxyspider.spider import Spider


class XiciSpider(Spider):
    def __init__(self, page_no=10):
        super(Spider, self).__init__()
        self.page_no = page_no

    def crawl_urls(self):
        return ['http://www.xicidaili.com/nn/{}'.format(i) for i in range(1, self.page_no + 1)]

    def crawl_callback(self, text):
        proxies = []
        soup = bs(text, 'lxml')
        for tr in soup.table.findAll('tr'):
            td = list(tr.findAll('td'))
            if len(td) == 10:
                ip = td[1].string
                port = td[2].string
                method = td[5].string
                if method == 'HTTP':
                    proxies.append('{}:{}'.format(ip, port))
        return proxies
