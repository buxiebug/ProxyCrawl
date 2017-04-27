# encoding:utf8
__author__ = 'brianyang'

import sys

reload(sys)
sys.setdefaultencoding('utf8')
import json
import random
from multiprocessing import Pool
from datetime import datetime

import requests

from bt_config import *
from util import solr_util
from config import bt_dir, bt_download_queue, bt_solr_address, proxy_queue
from proxy import Proxy
from util.mq_util import basic_consume_msg, basic_publish_msg

os.chdir('bts')

download_list = [
    'http://www.bttiantang.com/download1.php',
    'http://www.bttiantang.com/download2.php',
    'http://www.bttiantang.com/download3.php',
    'http://www.bttiantang.com/download4.php',
    'http://www.bttiantang.com/download5.php'
]


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


def download(session, film_id, uhash, download_path):
    try:
        data = {
            'action': 'download',
            'id': film_id,
            'uhash': uhash,
            'mageField.x': random.randint(1, 139),
            'mageField.y': random.randint(1, 54)
        }
        download_url = download_list[random.randint(0, len(download_list) - 1)]
        resp = session.post(download_url, data=data, timeout=10)
        if resp.status_code != 200:
            raise requests.ConnectionError()
        if 'text/html' in resp.headers['content-type']:
            if 'the torrent does not exist' in resp.text:
                print 'the torrent does not exist {}'.format(film_id)
                return True
        if 'application/octet-stream' not in resp.headers['content-type']:
            raise requests.ConnectionError()
        with open(download_path, 'w') as f:
            content = resp.content
            f.write(content)
        return True
    except requests.ConnectionError:
        return False
    except Exception, e:
        print e
        return False


def download_bt(info):
    title_md5 = info.get('title_md5')
    bts = info.get('bts')
    film_id = info.get('film_id')
    if not os.path.exists(title_md5):
        os.mkdir(title_md5)
    session = get_crawl_session()
    for bt in bts:
        download_path = '{}/{}/{}'.format(bt_dir, title_md5, bt.get('bt_uhash'))
        flag = download(session, film_id, bt.get('bt_uhash'), download_path)
        while not flag:
            Proxy.close_session(session)
            session = get_crawl_session()
            flag = download(session, film_id, bt.get('bt_uhash'), download_path)
    basic_publish_msg(proxy_queue, session.proxies.get('http'))
    Proxy.close_session(session)
    add_to_solr(info)


def add_to_solr(info):
    doc = {
        'id': info.get('film_id'),
        'title': info.get('title'),
        'update_time': info.get('update_time'),
        'source_url': '{}{}'.format('www.bttiantang.com', info.get('url')),
        'title_md5': info.get('title_md5'),
        'bt_info': [json.dumps(bt, ensure_ascii=False) for bt in info.get('bts')],
        'feed_time': str(datetime.now())
    }
    solr_util.add_item(bt_solr_address, doc)
    print 'add to solr done {}'.format(info.get('title'))


pool = Pool(processes=5)


def callback(ch, method, props, body):
    def ack_callback(result):
        ch.basic_ack(delivery_tag=method.delivery_tag)

    info = json.loads(body)
    pool.apply_async(download_bt, (info, ), callback=ack_callback)


basic_consume_msg(bt_download_queue, callback, prefetch_count=10)

pool.close()
pool.join()
