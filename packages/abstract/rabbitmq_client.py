#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/28 21:13
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import sys
import pika
import time
import signal
import threading

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

    def __init__(self):

        AbstractModule.__init__(self)

        self.host = ''
        self.port = 0
        self.consumer_conn = None
        self.producer_conn = None

        self.consumer_channel = None
        self.producer_channel = None

        self.internal_lock = threading.Lock()
        self.producer_conn_check_thread = None

        self._dismissed = threading.Event()

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def init(self, config_path=None):

        AbstractModule.init(self, config_path=config_path)

        self.init_client()

        return True

    def __del__(self):

        AbstractModule.__del__(self)

    def exit(self):

        AbstractModule.exit(self)

        self._dismissed.set()

        self.producer_conn_check_thread.join()

        return True
    
    def signal_handler(self, signum, frame):
        
        self.exit()

        sys.exit(0)

    def init_client(self):
        """
        """

        self.host = self.config['rmq_client']['host']
        self.port = int(self.config['rmq_client']['port'])

        self.consumer_conn = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        self.producer_conn = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))

        # why just initialize producer client rather than both consumer & producer clients here ?
        # for if we initialize consumer client here, then the consumer will start consuming, which is not expected.

        self.init_producer_client()

    def init_consumer_client(self):
        """
        """

        exchange_config = self.config['rmq_client']['consumer_exchange']
        self.consumer_exchange = Exchange(**exchange_config)

        if not self.consumer_exchange.exchange_name:
            return False

        self.consumer_channel = self.consumer_conn.channel()
        self.consumer_channel.exchange_declare(exchange=self.consumer_exchange.exchange_name,
                                               type=self.consumer_exchange.exchange_type)

        result = self.consumer_channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.consumer_channel.queue_bind(exchange=self.consumer_exchange.exchange_name, queue=queue_name,
                                         routing_key=self.consumer_exchange.routing_key)

        self.consumer_channel.basic_qos(prefetch_count=1)
        self.consumer_channel.basic_consume(self.callback, queue=queue_name)

        return True

    def init_producer_client(self):
        """Initialize producer client
        """

        exchange_config = self.config['rmq_client']['producer_exchange']
        self.producer_exchange = Exchange(**exchange_config)

        if not self.producer_exchange.exchange_name:
            return False

        self.producer_channel = self.producer_conn.channel()
        self.producer_channel.exchange_declare(exchange=self.producer_exchange.exchange_name,
                                               type=self.producer_exchange.exchange_type)

        self.producer_conn_check_thread = threading.Thread(target=self._process_data_events)
        self.producer_conn_check_thread.start()

        return True
    
    def _process_data_events(self):
        """
        """

        while True:

            if self._dismissed.is_set():
                break

            with self.internal_lock:
                self.producer_conn.process_data_events()

            time.sleep(1)

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
        if not self.consumer_channel:
            return False

        self.consumer_channel.start_consuming()

        return True

    def publish(self, exchange=None, routing_key='', body=''):
        """
        """

        if not self.producer_channel:
            return False

        # use default exchange if parameter exchange is None
        if exchange is None:
            exchange = self.producer_exchange.exchange_name

        with self.internal_lock:
            self.producer_channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

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
