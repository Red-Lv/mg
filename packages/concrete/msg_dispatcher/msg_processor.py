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

    def __init__(self):

        self.msg_dispatcher = None
        self.db = None

        self.pool_size = 20
        self.thread_pool = None

    def init(self, msg_dispatcher=None, pool_size=20):

        self.msg_dispatcher = msg_dispatcher
        if not self.msg_dispatcher:
            return False

        self.db = frame.mongo_db.fetch_dbhandler('db_test')
        if not self.db:
            return False

        self.pool_size = pool_size
        self.thread_pool = threadpool.ThreadPool(self.pool_size)

        return True

    def __del__(self):

        MsgDispatcher.__del__()

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
            msg_obj = None
            LOG_WARNING('fail to load msg to json. msg: %s', msg)
            return False

        if 'appid' not in msg_obj or 'eid' not in msg_obj:
            LOG_WARNING('msg does not have appid or eid. msg: %s', msg)
            return False

        LOG_INFO('start processing msg.')
        timestamp_s = int(time.time())

        # dump msg
        self.dump_msg(msg_obj)

        # route msg
        self.route_msg(msg_obj)

        timestamp_e = int(time.time())
        time_cost = timestamp_e - timestamp_s
        LOG_INFO('finish processing msg. time_cost: %s.', time_cost)

        return True

    def dump_msg(self, msg_obj):
        """

        msg_obj: json in unicode
        """

        if not self.db:
            LOG_WARNING('the db is None')
            return False

        collection = self.db['collection_msg']

        spec = {'appid': msg_obj['appid'], 'eid': msg_obj['eid']}
        msg = msg_obj.encode('UTF-8')

        collection.update(spec, msg, upsert=True)

        return True

    def route_msg(self, msg_obj):
        """

        msg_obj: json in unicode
        """

        msg = msg_obj.encode('UTF-8')

        time_set = msg_obj.get('time_set', 0)
        if time_set:
            # @TODO
            self.msg_dispatcher.publish(msg)
        else:
            # @TODO
            pass

        return True
