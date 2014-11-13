#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 22:00
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import json
import time
import threadpool

from lib.globals import *
from lib.singleton import *


class MsgProcessor(object):
    """
    """

    __metaclass__ = Singleton

    def __init__(self, msg_to_publish=None):

        self.db = None
        self.msg_to_publish = msg_to_publish

        self.pool_size = 20
        self.thread_pool = None

    def init(self, pool_size=20):

        if not self.msg_to_publish:
            LOG_WARNING('msg_to_publish queue is None')
            return False

        self.init_db()

        self.pool_size = pool_size
        self.thread_pool = threadpool.ThreadPool(self.pool_size)

        return True

    def init_db(self):

        self.msg_db = frame.mongo_db.fetch_dbhandler('db_message')

        return True

    def __del__(self):

        pass

    def exit(self):

        self.thread_pool.dismissWorkers(self.pool_size, do_join=True)

        return True

    def add_task(self, msg):

        request = threadpool.makeRequests(self.process_msg, (msg,))[0]

        self.thread_pool.putRequest(request)

        return True

    def wait(self):

        self.thread_pool.wait()

    def process_msg(self, msg=None):
        """
        """

        try:
            msg_obj = json.loads(msg)
        except Exception as e:
            LOG_WARNING('fail to load msg to json. msg: %s', msg)
            return False

        if 'appid' not in msg_obj or 'eid' not in msg_obj:
            LOG_WARNING('msg does not have appid or eid. msg: %s', msg)
            return False

        LOG_INFO('start processing msg. appid: %s, eid: %s', msg_obj['appid'], msg_obj['eid'])
        timestamp_s = time.time()

        # dump msg
        self.dump_msg(msg_obj)

        # route msg
        self.route_msg(msg_obj)

        timestamp_e = time.time()
        time_cost = timestamp_e - timestamp_s

        LOG_INFO('finish processing msg. time_cost: %s.', time_cost)

        return True

    def dump_msg(self, msg_obj):
        """

        msg_obj: json in unicode
        """

        if not self.msg_db:
            LOG_WARNING('the db is None')
            return False

        collection = self.msg_db['collection_msg']
        if collection.count() == 0:
            collection.ensure_index([('appid', pymongo.ASCENDING), ('eid', pymongo.ASCENDING)],
                                    unique=True, backgroud=True)

        spec = {'appid': msg_obj['appid'], 'eid': msg_obj['eid']}

        collection.update(spec, msg_obj, upsert=True)

        return True

    def route_msg(self, msg_obj):
        """

        msg_obj: json in unicode
        """

        msg = json_to_str(msg_obj)

        time_set = msg_obj.get('time_set', 0)
        if time_set:
            # @TODO
            pass
        else:
            self.msg_to_publish.put(('', msg))

        return True
