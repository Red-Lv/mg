#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/26 16:03
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import logging
import logging.config

from singleton import *


class Logger(object):
    """the logger module for the frame
    """

    __metaclass__ = Singleton

    def __init__(self):

        self.config = None
        self.logger = None
        self.wf_logger = None

    def init(self, config_path):

        self.read_config(config_path)

        self.logger = logging.getLogger('mg_logger')
        self.wf_logger = logging.getLogger('mg_wf_logger')

        return True

    def __del__(self):

        pass

    def exit(self):

        return True

    def read_config(self, config_path):
        """
        """

        logging.config.fileConfig(config_path)

        return True

if __name__ == '__main__':

    logger = Logger()
    logger.init('../conf/logger.conf')

    logger.logger.info('everything is ok')
    logger.wf_logger.error('something wrong')
