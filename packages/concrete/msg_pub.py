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


class MsgPub(RabbitMQClient):
    """
    """

    def __init__(self):

        RabbitMQClient.__init__(self)

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path)

        self.mysql_db = frame.mysql_db

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def publish(self):

        dir_info_list = self.get_dir_info_list()
        for dir_info in dir_info_list:
            msg = json.dumps(dir_info, encoding='GBK')
            RabbitMQClient.publish(self, msg.encode('UTF-8'))
            time.sleep(0.1)

        return True

    def get_dir_info_list(self):

        dir_info_list = []

        conn = self.mysql_db.fetch_dbhandler('db_ori_even')
        if not conn:
            return dir_info_list

        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('SELECT * FROM dir_ori_info0 LIMIT 1000')
        dir_info_list = cursor.fetchall()
        cursor.close()

        self.mysql_db.push_back_dbhandler(conn)

        return dir_info_list

if __name__ == '__main__':

    msg_pub = MsgPub()
    msg_pub.init('./conf/msg_pub.conf')

    msg_pub.publish()
