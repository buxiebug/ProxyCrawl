# encoding:utf8
__author__ = 'brianyang'

import sys

reload(sys)
sys.setdefaultencoding('utf8')
from bs4 import BeautifulSoup as bs
import requests
import json
from hashlib import md5
from multiprocessing.dummy import Pool

from bt_config import *
from proxy import Proxy
from util.mq_util import basic_consume_msg, basic_publish_msg
from config import bt_download_queue, proxy_queue

queue = 'bt_list'


def get_crawl_session():
    session = Proxy.get_session()
    while not session:
        session = Proxy.get_session()
    session.headers['Host'] = 'www.bttiantang.com'
    session.headers['Origin'] = 'http://www.bttiantang.com'
    session.headers[
        'Cookie'] = 'bdshare_firstime=1462778973268; CNZZDATA1257023383=1221493766-1462778970-%7C1462778970; adClass0803=3; CNZZDATA5933609=cnzz_eid%3D1804030634-1462776360-null%26ntime%3D1463119258; _gat=1; cscpvcouplet_fidx=3; CNZZDATA5934599=cnzz_eid%3D1502047167-1462777141-http%253A%252F%252Fwww.bttiantang.com%252F%26ntime%3D1463120023; ftcpvcouplet_fidx=1; 37cs_pidx=2; 37cs_pennding16053=true; CS_pending16053=true; 37cs_show=1%2C2; OpenClick110602=open; 37cs_user=37cs44109956943; CNZZDATA5934077=cnzz_eid%3D360329824-1462778255-%26ntime%3D1463119506; cscpvrich_fidx=2; _ga=GA1.2.1234924612.1462778978'
    session.headers['Referer'] = 'http://www.bttiantang.com'
    return session


def crawl_detail(info):
    try:
        session = get_crawl_session()
        if 'plus' in info.get('url', ''):
            return True
        url = 'http://www.bttiantang.com{}'.format(info.get('url'))
        resp = session.get(url, timeout=5)
        if resp.status_code != 200:
            raise requests.ConnectionError()
        if not resp.encoding:
            raise requests.ConnectionError()
        text = resp.text.encode(resp.encoding)
        Proxy.close_session(session)
        soup = bs(text, 'lxml')
        if not soup.find('div', class_='moviedteail_tt'):
            raise requests.ConnectionError()
        tinfo = soup.find('div', class_='tinfo')
        if not tinfo:
            print 'not find tinfo: {}'.format(url)
            return True
        alert = tinfo.find('p', class_='alert')
        if alert:
            print 'alert: {}'.format(url)
            return True
        bts = soup.find_all('div', class_='tinfo')
        if len(bts) < 1:
            raise Exception("bt length is wrong:{}".format(url))
        title = info.get('title')
        title_md5 = md5(title).hexdigest()
        info['title_md5'] = title_md5
        info['bts'] = list()
        for bt in bts:
            href = bt.find('a')['href']
            params = href.split('&')
            bt_title = bt.find('a').find('p', class_='torrent').text
            film_id = params[2].split('=')[1]
            bt_uhash = params[3].split('=')[1]
            info['film_id'] = film_id
            info['bts'].append({'bt_title': bt_title, 'bt_uhash': bt_uhash})
        print '{} done'.format(title)
        basic_publish_msg(proxy_queue, session.proxies.get('http'))
        basic_publish_msg(bt_download_queue, json.dumps(info))
        return True
    except requests.ConnectionError:
        return False
    except Exception, e:
        print e
        return False


if not os.path.exists('bts'):
    os.mkdir('bts')
os.chdir('bts')


def callback(ch, method, props, body):
    info = json.loads(body)
    flag = crawl_detail(info)
    while (not flag):
        flag = crawl_detail(info)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def begin_crawl_detail(queue, callback):
    basic_consume_msg(queue, callback)


pool = Pool(processes=5)
for i in range(5):
    pool.apply_async(begin_crawl_detail, (queue, callback))
pool.close()
pool.join()
