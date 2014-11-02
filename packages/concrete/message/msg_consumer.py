#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/28 23:51
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import json
import time

from packages.abstract.abstract_module import *
from packages.abstract.rabbitmq_client import *


class MsgConsumer(RabbitMQClient):
    """
    """

    def __init__(self):

        RabbitMQClient.__init__(self)

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path)

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def callback(self, ch, method, properties, body):

        obj = json.loads(body)

        book_name = obj['book_name'].encode('GBK')
        print 'msg received: {0}'.format(book_name)

        LOG_INFO(book_name)

        ch.basic_ack(delivery_tag=method.delivery_tag)

        return True

if __name__ == '__main__':

    msg_consumer = MsgConsumer()
    msg_consumer.init('./conf/msg_consumer.conf')

    msg_consumer.consume()
