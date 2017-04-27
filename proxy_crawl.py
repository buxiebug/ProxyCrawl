# coding:utf8
import sys
import time
from multiprocessing.dummy import Pool

import click
import requests

reload(sys)
sys.setdefaultencoding('utf-8')

from proxy import Proxy
from util.mq_util import basic_consume_msg, basic_publish_msg
from util.proxy_util import valid_proxy
from config import proxy_queue, proxy_candi_queue

retry_num = 3

test_url = 'http://p3.qhimg.com/t01b7cb83f41d31a8bf.png'


class ProxyCrawl(object):
    def __init__(self):
        self.crawl_spiders = []
        self.pool = Pool(processes=5)
        self.dup_pick_set = set()
        self.valid_set = set()

    def register(self, crawl_spider):
        self.crawl_spiders.append(crawl_spider)

    def begin_crawl(self):
        for crawl_spider in self.crawl_spiders:
            for url in crawl_spider.crawl_urls():
                self.pool.apply_async(self.__crawl, (url, crawl_spider.crawl_callback))
        self.__close()

    def __crawl(self, crawl_url, callback):
        try:
            session = Proxy.get_session()
            if not session:
                raise requests.ConnectionError()
            print 'using {}'.format(session.proxies)
            print 'crawling {}'.format(crawl_url)
            result = session.get(crawl_url, timeout=3)
            for i in range(retry_num):
                if result.status_code != 200:
                    Proxy.change_proxy(session)
                    result = session.get(crawl_url, timeout=3)
                else:
                    break
            if result.status_code != 200:
                raise requests.ConnectionError()
            Proxy.close_session(session)
            proxies = callback(result.text)
            for proxy in proxies:
                self.check_add(proxy)
            print 'crawling {} done'.format(crawl_url)
            return
        except Exception, e:
            print 'retrying'
            Proxy.close_session(session)
            time.sleep(3)
            self.__crawl(crawl_url, callback)

    def check_add(self, proxy_str):
        if proxy_str in self.dup_pick_set:
            return
        self.dup_pick_set.add(proxy_str)
        basic_publish_msg(proxy_candi_queue, proxy_str)

    def __close(self):
        self.pool.close()
        self.pool.join()


@click.group(help=u'获取http代理')
def cli():
    pass


def callback(ch, method, prop, body):
    if valid_proxy({'http': body}, 'http://www.bttiantang.com'):
        print '{} is valid'.format(body)
        basic_publish_msg(proxy_queue, body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


@click.command(name='valid', help=u'监测代理可用性')
def check():
    basic_consume_msg(proxy_candi_queue, callback, prefetch_count=10)


@click.command(name='run', help=u'抓取代理ip')
def run():
    proxy_crawl = ProxyCrawl()
    from proxyspider.xici_spider import XiciSpider
    xici_spider = XiciSpider(10)
    proxy_crawl.register(xici_spider)
    from proxyspider.liunian_spider import LiunianSpider

    proxy_crawl.register(LiunianSpider(100))
    proxy_crawl.begin_crawl()
    print 'crawl done'
    print 'crawling {} proxies'.format(len(proxy_crawl.dup_pick_set))


cli.add_command(run)
cli.add_command(check)

if __name__ == '__main__':
    cli()
