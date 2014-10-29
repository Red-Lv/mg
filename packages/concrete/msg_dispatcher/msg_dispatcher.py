#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 21:41
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

from lib.globals import *
from packages.abstract.rabbitmq_client import *
from packages.concrete.msg_dispatcher import *


class MsgDispatcher(RabbitMQClient):
    """Message dispatcher

    task:
    1、store the message
    2、route the message to dispatcher or to message_sender
    """

    def __init__(self):

        RabbitMQClient.__init__(self)

        self.msg_processor = None

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path)

        db = frame.mongo_db.fetch_dbhandler('db_msg')
        collection = db['collection_msg']
        self.msg_processor = MsgProcessor(collection, self.msg_router)
        self.msg_processor.init()

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def callback(self, ch, method, properties, body):
        """
        """

        self.msg_processor.add_task(msg)

        return True

    def msg_router(self, msg, time_set=0):
        """
        """

        if time_set:
            # @TODO
            self.publish(msg)
        else:
            # @TODO
            #self.publish(self.config['direct_pub_exchange'], msg)
            pass

        return True
