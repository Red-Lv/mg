#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/26 15:10
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import sys
import configobj

from lib.singleton import *
from lib.logger import *
from lib.db import *


class Frame(object):
    """framework of the mg module

    This frame is to create the running environment necessary for modules.

    """

    __metaclass__ = Singleton

    def __init__(self):

        self.config = None

    def init(self, config_path=None):
        """Initialize the frame
        """

        if not self.read_config(config_path):
            print 'fail to read config'
            return False

        if not self.init_logger():
            print 'fail to initialize logger module'
            return False

        if not self.init_db():
            print 'fail to initialize db module'
            return False

        return True

    def __del__(self):

        pass

    def exit(self):

        return True

    def read_config(self, config_path=None):
        """
        """

        if not config_path:
            return False

        self.config = configobj.ConfigObj(config_path)

        return True

    def init_logger(self):
        """
        """

        logger_config_path = self.config['logger']['logger_config_path']

        self.logger = Logger()
        self.logger.init(logger_config_path)

        return True

    def init_db(self):
        """
        """

        db_config_path = self.config['db']['db_config_path']

        self.mysql_db = MySqlDB()
        self.mysql_db.init(db_config_path)

        self.mongo_db = MongoDB()
        self.mongo_db.init(db_config_path)

        return True
