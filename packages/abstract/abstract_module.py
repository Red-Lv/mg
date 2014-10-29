#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/28 13:38
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import configobj

from lib.global import *


class AbstractModule(object):
    """Abstract module
    """

    def __init__(self, *args, **kwargs):

        self.config = None

    def init(self, *args, **kwargs):

        config_path = kwargs.get('config_path')
        self.read_config(config_path)

        return True

    def __del__(self, *args, **kwargs):

        pass

    def exit(self, *args, **kwargs):

        return True

    def read_config(self, config_path=None):
        """
        """

        if not config_path:
            return False

        self.config = configobj.ConfigObj(config_path)

        return True
