#-*- encoding: gbk -*-

'''
@module :   bin.public.NovelLog
@date   :   2012-8-20
@author :   wengyanqing
@brief	:	Novel日志操作封装

NovelLog Usage
1. 调用init_main_log初始化框架Log，仅能初始化一次
2. 调用init_module_log初始化每个模块的log，每个模块调用一次，传入模块的__module__作为module_key
3. init后调用LOG_XXX输出log即会自动定位到模块对应的日志文件中
'''

import sys
import os
import time
import logging
import traceback
import string
import subprocess

MAIN_LOG_KEY = 'novel_main_log_key'

# 全局日志管理， 模块日志输出定位
module_logger = {}
file_inode_dict = {}

check_point = 0

# 自定义log_level
levelinfo = {
    'fatal'		:	logging.WARNING+1,
    'warning'	:	logging.WARNING,
    'notice'	:	logging.DEBUG+2,
    'trace'		:	logging.DEBUG+1,
    'debug'		:	logging.DEBUG,
    }

logging.addLevelName(logging.DEBUG+1, 'TRACE')
logging.addLevelName(logging.DEBUG+2, 'NOTICE')
logging.addLevelName(logging.WARNING+1, 'FATAL')

def custom_log_factory(logger, custom_level):
    def custom_log(msg, *args, **kwargs):
        if logger.level > custom_level:
            return
        logger._log(custom_level, msg, args, kwargs)

    return custom_log


def fetch_file_inode(_file):

    cmd = "stat {0} | grep -o 'Inode: [^ ]*' | awk -F: '{{print $2}}' | tr -d ' '".format(_file)
    child = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (stdout, stderr) = child.communicate()

    if stderr:
        return False, ''

    return True, stdout

# 框架调用，用户禁止调用
def init_module_log(module_key, module_name, level, pathfile):

    if module_logger:
        return

    level = levelinfo[level]
    formatter = logging.Formatter('%(levelname)s: %(asctime)s: '+ module_name + ' * %(process)d [ logid:%(process)d ] %(message)s', '%m-%d %H:%M:%S')

    # 普通日志logger
    logger = logging.getLogger(module_name + 'log')
    handler = logging.FileHandler(pathfile + 'log')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False

    (flag, inode) = fetch_file_inode(handler.baseFilename)
    file_inode_dict[handler.baseFilename] = inode

    # 错误日志logger
    wflogger = logging.getLogger(module_name + 'wflog')
    wfhandler = logging.FileHandler(pathfile + 'log.wf')
    wfhandler.setFormatter(formatter)
    wflogger.addHandler(wfhandler)
    wflogger.setLevel(level)
    wflogger.propagate = False

    (flag, inode) = fetch_file_inode(wfhandler.baseFilename)
    file_inode_dict[wfhandler.baseFilename] = inode

    # 设置模块自定义log_level属性
    setattr(logger, 'trace', custom_log_factory(logger, logging.DEBUG+1))
    setattr(logger, 'notice', custom_log_factory(logger, logging.DEBUG+2))
    setattr(wflogger, 'fatal', custom_log_factory(wflogger, logging.WARNING+1))

    # logger & wflogger加入全局日志管理
    # module_key仅保留最后一级
    if module_key != MAIN_LOG_KEY:
        module_key = string.split(module_key, '.')[-1]

    module_logger[module_key] = (logger, wflogger)

def check_validity(logger, wflogger):

    global check_point

    cur_time = time.time()
    cur_check_point = int(cur_time / (5 * 60))

    if cur_check_point == check_point:
        return

    check_point = cur_check_point

    reinit_handler(logger)
    reinit_handler(wflogger)

def reinit_handler(logger):

    if not logger:
        return

    handler = logger.handlers[0]
    _file = handler.baseFilename
    (flag, inode) = fetch_file_inode(_file)

    if flag and file_inode_dict[_file] == inode:
        return

    file_inode_dict[_file] = inode
    handler.close()

def init_main_log(name, level, pathfile):
    init_module_log(MAIN_LOG_KEY, name, level, pathfile)

def get_module_logger():

    logger, wflogger = None, None
    flag = False

    for stack in reversed(traceback.extract_stack()):
        filename = stack[0]
        module_name = string.split(filename[0:len(filename)-len('.py')], os.sep)[-1]
        if module_logger.has_key(module_name):
            logger, wflogger = module_logger[module_name]
            flag = True
            break

    # 非模块logger，返回框架logger
    if not flag:
        if module_logger.has_key(MAIN_LOG_KEY):
            logger, wflogger = module_logger[MAIN_LOG_KEY]

    check_validity(logger, wflogger)

    return logger, wflogger

# 在log message中增加调用者的文件和对应行号
def format_log_msg(message):
    stack = traceback.extract_stack()
    # -1:format_log_msg
    # -2:LOG_XXXX
    # -3:caller of LOG_XXXX
    filename = stack[-3][0]
    filename = string.split(filename, os.sep)[-1]
    lineno = stack[-3][1]
    return '[ %s:%s ] %s' % (filename, lineno, message)


# 向module模块的日志中输出日志（用户调用）
def LOG_DEBUG(msg, *args, **kwargs):
    (logger, wflogger) = get_module_logger()
    if logger:
        logger.debug(format_log_msg(msg), *args, **kwargs)

def LOG_TRACE(msg, *args, **kwargs):
    (logger, wflogger) = get_module_logger()
    if logger:
        logger.trace(format_log_msg(msg), *args, **kwargs)

def LOG_NOTICE(msg, *args, **kwargs):
    (logger, wflogger) = get_module_logger()
    if logger:
        logger.notice(format_log_msg(msg), *args, **kwargs)

def LOG_WARNING(msg, *args, **kwargs):
    (logger, wflogger) = get_module_logger()
    if wflogger:
        wflogger.warning(format_log_msg(msg), *args, **kwargs)

def LOG_FATAL(msg, *args, **kwargs):
    (logger, wflogger) = get_module_logger()
    if wflogger:
        wflogger.fatal(format_log_msg(msg), *args, **kwargs)


def main(argv=None):
    module = 'test'
    init_main_log(module, 'debug', '../../log/test')

    LOG_DEBUG('hello debug')
    LOG_TRACE('hello trace')
    LOG_NOTICE('hello notice')
    LOG_WARNING('hello warning')
    LOG_FATAL('hello fatal')


if __name__ == '__main__':
    ret = main()
    sys.exit(ret)

