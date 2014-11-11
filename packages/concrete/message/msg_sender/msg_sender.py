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
        if not self.url:
            LOG_WARNING('url to post is None')
            return False

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def callback(self, ch, method, properties, body):

        LOG_DEBUG('msg received: [body: %s]', body)

        try:
            r = requests.post(url=self.url, data={'data': body})
            ret = r.json()
        except Exception as e:
            ret = {'error_message': e}

        if ret.get('status') != 0:
            LOG_WARNING('fail to post data to push service. push_service: %s, error: %s',
                        self.url, ret.get('error_message'))
        else:
            LOG_INFO('success in posting data to push service.')

        ch.basic_ack(delivery_tag=method.delivery_tag)

        return True

if __name__ == '__main__':

    msg_sender = MsgSender()
    msg_sender.init('./conf/msg_sender.conf')

    msg_sender.consume()
