#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 00:34
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

from lib.frame import *

frame = Frame()
frame.init('./conf/frame.conf')


def LOG_DEBUG(*args, **kwargs):

    frame.logger.debug(*args, **kwargs)


def LOG_INFO(*args, **kwargs):

    frame.logger.info(*args, **kwargs)


def LOG_WARNING(*args, **kwargs):

    frame.logger.warning(*args, **kwargs)


def LOG_ERROR(*args, **kwargs):

    frame.logger.error(*args, **kwargs)


def LOG_CRITICAL(*args, **kwargs):

    frame.logger.critical(*args, **kwargs)
