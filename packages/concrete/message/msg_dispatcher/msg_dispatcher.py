#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 21:41
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import Queue
import threading

from lib.globals import *

from packages.abstract.rabbitmq_client import *
from packages.concrete.message.msg_dispatcher.msg_processor import *


class MsgDispatcher(RabbitMQClient):
    """Message dispatcher

    task:
    1、store the message
    2、route the message to message_timer or to message_sender
    """

    __metaclass__ = Singleton

    def __init__(self):

        RabbitMQClient.__init__(self)

        self.msg_to_publish = Queue.Queue()
        self.msg_processor = MsgProcessor(self.msg_to_publish)

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path)

        self.msg_processor.init()

        self.publish_thread = threading.Thread(target=self.publish)

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def callback(self, ch, method, properties, body):
        """
        """

        LOG_DEBUG('msg received: [body: %s]', body)

        self.msg_processor.add_task(body)

        ch.basic_ack(delivery_tag=method.delivery_tag)

        return True

    def publish(self):
        """
        """

        while True:

            if self._dismissed.is_set():
                break

            routing_key, msg = self.msg_to_publish.get()
            RabbitMQClient.publish(self, routing_key=routing_key, body=msg)

            LOG_DEBUG('msg published: [routing_key: %s, body: %s]', routing_key, msg)

        return True

    def consume(self):
        """
        """

        RabbitMQClient.consume(self)

    def run(self):
        """
        """

        # invoke the publish thread
        self.publish_thread.start()

        # invoke the consume thread
        self.consume()

        return True


if __name__ == '__main__':

    msg_dispatcher = MsgDispatcher()
    msg_dispatcher.init('./conf/msg_dispatcher.conf')

    msg_dispatcher.run()
