#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/26 15:10
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import sys
import ConfigParser

from lib.singleton import *


class Frame(object):
    """framework of the MG module

    This frame is to create the running environment necessary for modules.

    """

    __metaclass__ = Singleton

    def __init__(self):

        pass

    def init(self, config_path):
        """Initialize the frame
        """

        self.read_config(config_path)

        if not self.init_logger():
            print 'fail to initialize logger module.'
            return False

        if not self.init_db():
            print 'fail to initialize db module.'
            return False

        return True

    def __del__(self):

        pass

    def exit(self):

        return True

    def read_config(self, config_path):
        """
        """

        self.config = ConfigParser.SafeConfigParser()
        self.config.read(config_path)

        return True

    def init_logger(self):
        """
        """

        logger_home = self.config.get('logger', 'logger_home')
        logger_file = self.config.get('logger', 'logger_file')
        logger_level = self.config.get('logger', 'logger_level')

        init_main_log(logger_level, self.log_path + os.sep + logger_file)

        return True
