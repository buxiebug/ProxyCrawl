# encoding:utf8
__author__ = 'brianyang'

import ConfigParser
import codecs

import pika

configParser = ConfigParser.RawConfigParser()
configParser.readfp(codecs.open('/home/q/ProxyCrawl/conf/app.conf'))

rabbit_host_str = configParser.get('RabbitMQ', 'host')
rabbit_port_str = configParser.getint('RabbitMQ', 'port')

rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host_str, port=rabbit_port_str))

proxy_candi_queue = 'candi_proxy'
proxy_queue = 'valid_proxy'
bt_download_queue = 'bt_download_queue'

bt_solr_address = 'http://localhost:8983/solr/bt'
bt_dir = '/home/q/ProxyCrawl/btcrawl/bts'
