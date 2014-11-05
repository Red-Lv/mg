#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/30 00:55
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import requests

from lib.globals import *
from packages.abstract.rabbitmq_client import *


class MsgSender(RabbitMQClient):
    """Message sender
    """

    def __init__(self):

        RabbitMQClient.__init__(self)

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path)

        self.url = self.config['msg_sender']['url']

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def callback(self, ch, method, properties, body):

        # @TODO
        # post body to push module
        #

        print 'msg_sender receive msg. msg: {0}'.format(body)

        try:
            r = requests.post(url=self.url, data={'data': body})
            ret = r.json()
        except Exception as e:
            ret = {}

        if ret.get('status') == 0:
            print 'Success'
        else:
            print 'Fail'

        LOG_INFO('msg_sender receive msg. msg: %s', body)

        ch.basic_ack(delivery_tag=method.delivery_tag)

        return True

if __name__ == '__main__':

    msg_sender = MsgSender()
    msg_sender.init('./conf/msg_sender.conf')

    msg_sender.consume()
