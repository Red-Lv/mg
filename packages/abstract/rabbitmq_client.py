#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/28 21:13
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import pika
import Queue

from abstract_module import *


class Exchange(object):

    def __init__(self, *args, **kwargs):
        """
        """

        for key, value in kwargs.items():
            setattr(self, key, value)


class RabbitMQClient(AbstractModule):
    """Abstract RabbitMQ client
    """

    def __init__(self, consumer_channel_no=1, producer_channel_no=20):

        AbstractModule.__init__(self)

        self.config = None

        self.host = ''
        self.port = 0
        self.mq_conn = None

        self.consumer_channel_no = consumer_channel_no
        self.producer_channel_no = producer_channel_no

        self.consumer_channel_queue = Queue.Queue()
        self.producer_channel_queue = Queue.Queue()

    def init(self, config_path=None):

        AbstractModule.init(self, config_path=config_path)

        self.init_client()

        return True

    def __del__(self):

        AbstractModule.__del__(self)

    def exit(self):

        if self.mq_conn:
            self.mq_conn.close()

        return True

    def init_client(self):
        """
        """

        self.host = self.config['rmq_client']['host']
        self.port = int(self.config['rmq_client']['port'])
        self.mq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))

        self.init_producer_client()

    def init_consumer_client(self):
        """
        """

        exchange_config = self.config['rmq_client']['consumer_exchange']
        self.consumer_exchange = Exchange(**exchange_config)

        if not self.consumer_channel_no or not self.consumer_exchange.exchange_name:
            return False

        for i in xrange(self.consumer_channel_no):
            self.consumer_channel_queue.put(self.mq_conn.channel())

        channel = self.consumer_channel_queue.get()
        channel.exchange_declare(exchange=self.consumer_exchange.exchange_name,
                                 type=self.consumer_exchange.exchange_type)

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=self.consumer_exchange.exchange_name, queue=queue_name,
                           routing_key=self.consumer_exchange.routing_key)

        self.consumer_channel_queue.put(channel)

        return True

    def init_producer_client(self):
        """
        """

        exchange_config = self.config['rmq_client']['producer_exchange']
        self.producer_exchange = Exchange(**exchange_config)

        if not self.producer_channel_no or not self.producer_exchange.exchange_name:
            return False

        for i in xrange(self.producer_channel_no):
            self.producer_channel_queue.put(self.mq_conn.channel())

        channel = self.producer_channel_queue.get()
        channel.exchange_declare(exchange=self.producer_exchange.exchange_name,
                                          type=self.producer_exchange.exchange_type)

        channel.basic_qos(prefetch_count=10)
        channel.basic_consume(self.callback, queue=queue_name)

        self.producer_channel_queue.put(channel)

        return True

    def callback(self, ch, method, properties, body):
        """
        """

        print 'consuming msg: {0}'.format(body)

        return True

    def consume(self):
        """
        """

        self.init_consumer_client()

        channel = self.consumer_channel_queue.get()

        channel.start_consuming()

        self.consumer_channel_queue.put(channel)

        return True

    def publish(self, exchange=None, routing_key='', body=''):
        """
        """

        channel = self.producer_channel_queue.get()

        if exchange is None:
            exchange = self.producer_exchange.exchange_name

        self.producer_channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

        self.producer_channel_queue.put(channel)

        return True

if __name__ == '__main__':

    rmq_client = RabbitMQClient()
    rmq_client.init('../../conf/rmq_client.conf')

    import time
    for i in xrange(100):

        msg = 'blowing in the wind. {0}'.format(i)
        print msg
        rmq_client.publish(body=msg)

        time.sleep(1)

    rmq_client.consume()
