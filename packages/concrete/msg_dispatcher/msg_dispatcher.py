#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 21:41
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

from lib.globals import *
from lib.singleton import *

from packages.abstract.rabbitmq_client import *
from packages.concrete.msg_dispatcher.msg_processor import *


class MsgDispatcher(RabbitMQClient):
    """Message dispatcher

    task:
    1、store the message
    2、route the message to message_timer or to message_sender
    """

    __metaclass__ = Singleton

    def __init__(self):

        RabbitMQClient.__init__(self)

        self.msg_processor = None

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path)

        self.msg_processor = MsgProcessor()
        self.msg_processor.init(self)

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def callback(self, ch, method, properties, body):
        """
        """

        self.msg_processor.add_task(body)

        return True
