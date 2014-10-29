#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 22:00
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import time
import threadpool

from lib.singleton import *
from lib.globals import *


class MsgProcessor(object):
    """
    """

    __metaclass__ = Singleton

    def __init__(self, collection=None, msg_router=None):

        self.collection = collection
        self.msg_router = msg_router

    def init(self, pool_size=20):

        if not collection:
            return False

        self.pool_size = pool_size
        self.thread_pool = threadpool.ThreadPool(self.pool_size)

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
            msg_obj = None
            LOG_WARNING('fail to loads msg to json. msg: %s', msg)
            return False

        if 'appid' not in msg_obj or 'eid' in msg_obj:
            LOG_WARNING('msg does not have appid or eid. msg: %s', msg)
            return False

        LOG_INFO('start processing msg.')
        timestamp_s = int(time.time())

        self.dump_msg({'appid': msg_obj['appid'], 'eid': msg_obj['eid']}, msg)

        # route msg according to time_set value
        time_set = msg.get('time_set', 0)
        self.msg_router(msg, time_set)

        timestamp_e = int(time.time())
        time_cost = timestamp_e - timestamp_s
        LOG_INFO('finish processing msg. time_cost: %s.', time_cost)

        return True

    def dump_msg(self, spec, msg):
        """
        """

        self.collection.update(spec, msg, upsert=True)

        return True
