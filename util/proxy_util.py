# encoding:utf8
__author__ = 'brianyang'

import time

import requests

from config import proxy_queue, proxy_candi_queue
from mq_util import basic_consume_msg_once, basic_publish_msg


def valid_proxy(proxies, url='http://www.qunar.com'):
    try:
        resp = requests.head(url, proxies=proxies, timeout=1)
        if resp.status_code == 200:
            return True
    except Exception, e:
        return False
    return False


def get_proxy():
    retry_count = 0
    while retry_count < 5:
        proxy = basic_consume_msg_once(proxy_queue)
        if proxy:
            basic_publish_msg(proxy_candi_queue, proxy)
            return proxy
            break
        print 'retrying to find a valid proxy: {} times'.format(retry_count + 1)
        time.sleep(retry_count)
        retry_count += 1
    print 'Could not find a valid proxy'
    return None


if __name__ == '__main__':
    print get_proxy()
