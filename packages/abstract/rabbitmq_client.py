#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/28 21:13
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import pika

from abstract_module import *


class RabbitMQClient(AbstractModule):
    """abstract RabbitMQ client
    """

    def __init__(self):

        AbstractModule.__init__(self)

        self.config = None
        self.mq_conn = None
        self.sub_channel = None
        self.pub_channel = None

    def init(self, config_path=None):

        AbstractModule.init(self, config_path=config_path)

        self.init_client()

        return True

    def __del__(self):

        AbstractModule.__del__()

    def exit(self):

        if self.mq_conn:
            self.mq_conn.close()

        return True

    def init_client(self):
        """
        """

        mq_host, mq_port = self.config['rmq_client']['host'], self.config['rmq_client']['port']
        self.mq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=mq_host, port=int(mq_port)))

        self.init_sub_client()
        self.init_pub_client()

    def init_sub_client(self):
        """
        """

        self.sub_channel = self.mq_conn.channel()

        sub_exchange = self.config['rmq_client']['sub_exchange']
        self.sub_channel.exchange_declare(exchange=sub_exchange, type='fanout')

        result = self.sub_channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.sub_channel.queue_bind(exchange=sub_exchange, queue=queue_name)

        self.sub_channel.basic_consume(self.callback, queue=queue_name, no_ack=True)

        return True

    def init_pub_client(self):
        """
        """

        self.pub_channel = self.mq_conn.channel()

        pub_exchange = self.config['rmq_client']['pub_exchange']
        self.pub_channel.exchange_declare(exchange=pub_exchange, type='fanout')

        return True

    def callback(self, ch, method, properties, body):
        """
        """

        print 'consuming msg: {0}'.format(body)

        return True

    def consume(self):
        """
        """

        if not self.sub_channel:
            return False

        self.sub_channel.start_consuming()

        return True

    def publish(self, msg):
        """
        """

        if not self.pub_channel:
            return False

        pub_exchange = self.config['rmq_client']['pub_exchange']
        self.pub_channel.basic_publish(exchange=pub_exchange, routing_key='', body=msg)

        return True

if __name__ == '__main__':

    rmq_client = RabbitMQClient()
    rmq_client.init('../../conf/rmq_client.conf')

    import time
    for i in xrange(100):

        msg = 'blowing in the wind. {0}'.format(i)
        print msg
        rmq_client.publish(msg)

        time.sleep(1)

    rmq_client.consume()
