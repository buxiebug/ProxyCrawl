# encoding:utf8
__author__ = 'brianyang'

import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import json
import re

from bs4 import BeautifulSoup as bs
from multiprocessing.dummy import Pool
import click
import requests

from bt_config import *
from proxy import Proxy
from util.mq_util import basic_publish_msg
from config import bt_solr_address

list_queue = 'bt_list'

dup_set = set()

crawl_url_template = 'http://www.bttiantang.com/?PageNo={}'



def get_crawl_session():
    session = Proxy.get_session()
    while not session:
        session = Proxy.get_session()
    session.headers['host'] = 'www.bttiantang.com'
    session.headers['origin'] = 'http://www.bttiantang.com'
    session.headers['cookie'] = 'bdshare_firstime=1462778973268; cnzzdata1257023383=1221493766-1462778970-%7c1462778970; adclass0803=3; cnzzdata5933609=cnzz_eid%3d1804030634-1462776360-null%26ntime%3d1463119258; _gat=1; cscpvcouplet_fidx=3; cnzzdata5934599=cnzz_eid%3d1502047167-1462777141-http%253a%252f%252fwww.bttiantang.com%252f%26ntime%3d1463120023; ftcpvcouplet_fidx=1; 37cs_pidx=2; 37cs_pennding16053=true; cs_pending16053=true; 37cs_show=1%2c2; openclick110602=open; 37cs_user=37cs44109956943; cnzzdata5934077=cnzz_eid%3d360329824-1462778255-%26ntime%3d1463119506; cscpvrich_fidx=2; _ga=ga1.2.1234924612.1462778978'
    session.headers['referer'] = 'http://www.bttiantang.com'
    return session


def get_page_size():
    try:
        session = get_crawl_session()
        text = session.get('http://www.bttiantang.com', timeout=5).text
        Proxy.close_session(session)

        soup = bs(text, 'lxml')

        page_info = soup.find('span', class_='pageinfo').text

        s = re.search(u'(\d+)页', page_info)

        total_page = s.group(1)

        print 'total list page is {}'.format(total_page)
        return int(total_page)
    except Exception, e:
        print e
        return get_page_size()


def crawl_list_by_no(page_no, infos):
    try:
        crawl_url = crawl_url_template.format(page_no)
        session = get_crawl_session()
        result = session.get(crawl_url, timeout=5).text
        Proxy.close_session(session)
        soup = bs(result, 'lxml')
        items = soup.find_all('div', class_='item cl')
        for item in items:
            update_time = item.find('span')
            if update_time:
                update_time = update_time.text
            if item.find('a'):
                title = item.find('a').text
                url = item.find('a')['href']
                if url not in dup_set:
                    dup_set.add(url)
                    ddict = {
                        'title': title,
                        'update_time': update_time,
                        'url': url
                    }
                    infos.append(ddict)
                    basic_publish_msg(list_queue, json.dumps(ddict))

    except Exception, e:
        print e
        crawl_list_by_no(page_no)


def crawl_list_by_no_batch(page_no, infos, urls):
    try:
        crawl_url = crawl_url_template.format(page_no)
        session = get_crawl_session()
        result = session.get(crawl_url, timeout=5).text
        Proxy.close_session(session)
        soup = bs(result, 'lxml')
        items = soup.find_all('div', class_='item cl')
        for item in items:
            update_time = item.find('span')
            if update_time:
                update_time = update_time.text
            if item.find('a'):
                title = item.find('a').text
                url = item.find('a')['href']
                if url not in dup_set and url not in urls:
                    dup_set.add(url)
                    ddict = {
                        'title': title,
                        'update_time': update_time,
                        'url': url
                    }
                    infos.append(ddict)
                    basic_publish_msg(list_queue, json.dumps(ddict))

    except Exception, e:
        print e
        crawl_list_by_no(page_no)


@click.group(help=u'获取list页面')
def cli():
    pass


@click.command(name='all', help='crawl all')
def crawl_all():
    infos = list()
    page_size = get_page_size()
    pool = Pool(processes=3)
    for i in range(1, page_size + 1):
        pool.apply_async(crawl_list_by_no, (i, infos))
    pool.close()
    pool.join()
    write_file('list.csv', infos)


def get_exist_urls():
    search_url = '{}/{}'.format(bt_solr_address, "select?q=*%3A*&rows=30000&fl=source_url&wt=json&indent=true")
    print search_url
    docs = requests.get(search_url).json().get('response').get('docs')
    print len(docs)
    return [url.get('source_url')[url.get('source_url').index('/'):] for url in docs]


@click.command(name='batch', help='crawl batch')
def crawl_batch():
    infos = list()
    page_size = get_page_size()
    pool = Pool(processes=3)
    urls = get_exist_urls()
    for i in range(1, page_size + 1):
        pool.apply_async(crawl_list_by_no_batch, (i, infos, urls))
    pool.close()
    pool.join()
    write_file('batch.csv',infos)


def write_file(filename, infos):
    with open(filename, 'w') as f:
        for info in infos:
            f.write('{}\t{}\t{}\n'.format(info.get('title'), info.get('update_time'), info.get('url')))

cli.add_command(crawl_all)
cli.add_command(crawl_batch)

if __name__ == '__main__':
    cli()