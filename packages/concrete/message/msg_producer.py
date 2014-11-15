#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/28 23:51
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import json
import time

from lib.globals import *
from packages.abstract.abstract_module import *
from packages.abstract.rabbitmq_client import *


class MsgProducer(RabbitMQClient):
    """
    """

    def __init__(self):

        RabbitMQClient.__init__(self)

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path)

        self.mysql_db = frame.mysql_db
        self.mongo_db = frame.mongo_db

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def publish(self):

        entity_list = self.get_entity_list()
        for i in xrange(10000000):
            entity = entity_list[i % len(entity_list)]
            entity['last_content_index'] = int(time.time() * 1000)
            msg = json.dumps(entity, encoding='GBK', ensure_ascii=False)
            print msg.encode('UTF-8')
            RabbitMQClient.publish(self, body=msg.encode('UTF-8'))
            RabbitMQClient.publish(self, body=msg.encode('UTF-8'))
            time.sleep(0.001)
            LOG_INFO('msg published: %s', msg.encode('UTF-8'))

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

    def get_entity_list(self):

        entity_list = []

        db = self.mongo_db.fetch_dbhandler('db_entity_agg')
        if not db:
            return entity_list 

        collection = db['entity_agg_1']
        cursor = collection.find()
        for document in cursor:
            del(document['_id'])
            entity_list.append(document)

        cursor.close()
        self.mongo_db.push_back_dbhandler(db)

        return entity_list


if __name__ == '__main__':

    msg_producer = MsgProducer()
    msg_producer.init('./conf/msg_producer.conf')

    msg_producer.publish()
    msg_producer.exit()
