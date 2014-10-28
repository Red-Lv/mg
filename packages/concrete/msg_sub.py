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


class MsgSub(RabbitMQClient):
    """
    """

    def __init__(self):

        RabbitMQClient.__init__()

    def init(self, config_path=None):

        RabbitMQClient.init(config_path)

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit()

        return True

    def callback(self, ch, method, properties, body):

        print 'msg received: {0}'.formt(body)

        LOG_INFO(body)

if __name__ == '__main__':

    msg_sub = MsgSub()
    msg_sub.init('./conf/msg_sub.conf')

    msg_sub.consume()
