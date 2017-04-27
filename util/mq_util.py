# encoding:utf8
__author__ = 'brianyang'

import pika

host = 'localhost'
def get_channel(queue):
    #rabbit_connection = pika.BlockingConnection(pika.URLParameters('amqp://brian:jiubugaosuni199095@{}:5672/%2F'.format(host)))
    rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = rabbit_connection.channel()
    channel.queue_declare(queue)
    return channel


def basic_publish_msg(queue, body):
    channel = get_channel(queue)
    channel.basic_publish(exchange='',
                          routing_key=queue,
                          body=body)
    channel.close()


def basic_consume_msg_once(queue):
    channel = get_channel(queue)
    bg, bp, body = channel.basic_get(queue=queue, no_ack=False)
    if not bg:
        return None
    channel.basic_ack(delivery_tag=bg.delivery_tag)
    channel.close()
    return body


def basic_consume_msg(queue, callback, prefetch_count=1):
    channel = get_channel(queue)
    channel.basic_qos(prefetch_count=prefetch_count)
    channel.basic_consume(callback, queue=queue, no_ack=False)
    channel.start_consuming()
    channel.close()


def purge_queue(queue):
    channel = get_channel(queue)
    channel.queue_purge()
    channel.close()


if __name__ == '__main__':
    basic_publish_msg('candi_proxy', '58.49.22.233:8090')
    basic_publish_msg('candi_proxy', '14.125.62.79:8118')
    #purge_queue('bt_list')
    #from config import bt_download_queue
    #purge_queue(bt_download_queue)

    # time.sleep(1)
    # print basic_consume_msg_once('valid_proxy')
