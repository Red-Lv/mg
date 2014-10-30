#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 00:34
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

from lib.frame import *
from lib.utility import *

frame = Frame()
frame.init('./conf/frame.conf')


LOG_DEBUG = frame.logger.logger.debug
LOG_INFO = frame.logger.logger.info

LOG_WARNING = frame.logger.wf_logger.warning
LOG_DEBUG = frame.logger.wf_logger.error
LOG_CRITICAL = frame.logger.wf_logger.critical
