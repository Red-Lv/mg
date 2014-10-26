#!/usr/bin/env python
# -*- coding:GBK

'''
@module:MainFrame
@date: 2012-8-20
@author: zhangjianrong@baidu.com
@brief: 程序运行主框架
'''

import os
import sys
import ConfigParser
import importlib
import traceback
import signal
import subprocess
from DataFrame.public.FrameLog import *
from DataFrame.public.FrameDB import FrameDB
from DataFrame.public.FrameUtils import *
from DataFrame.public.LoadModuleParser import LoadModuleParser
from DataFrame.novel.data_sync.client_sync import ClientSync
from DataFrame.msgframe.MsgFrame import MsgFrame

def usage():
    msg = '''Usage:\tpython MainFrame.py conf_path [ modules ... ]
         启动规则：
         1、如果可选模块modules为空 ， 则按照 conf_path 中的conf 配置来启动
         2、其次，如果可选模块非法，则返回错误信息，停止执行
         3、再次，执行可选模块
      '''
    print msg

class MainFrame(object):
    """
        模块的主框架
    """
    def __init__(self):
        self.name = None
        self.config = None                 	# 全局配置文件句柄
        self.modules = []					# 运行模块集合
        self.noveldb = None					# NovelDB句柄
        self.top_novels = {}                # 热门小说
        self.status_file_path = ""          # 状态文件目录
        self.status_file = ""               # 状态文件
        self.time_file_path = ""            # 时间文件目录
        self.msg_frame = None               # 消息框架


    def init(self, confFile):
        """
            框架初始化函数
        """
        # 解析配置文件信息
        if not self.load_config(confFile):
            print "Failed to load configure file"
            return -1

        # 设置框架的name，用于设置时间，状态信息
        self.name = self.config.get("dataframe", "name")
        if len(self.name) == 0:
            print "There is no name for dataframe"
            return -1

        # 初始化日志模块
        if not self.init_log():
            print "Failed to initialize log info"
            return -2

        # 初始化状态信息
        '''
        if not self.set_status_info():
            print "Failed to initialize status info"
            return -3
            '''

        # 初始化时间信息
        '''
        if not self.set_time_info():
            print "Failed to initialize time info"
            return -4
            '''

        # 初始化数据库
        if not self.init_db():
            print "Failed to initialize database"
            return -5

        if not self.sync_data():
            print 'Failed to sync data'
            return -6

        '''
        # 初始化消息框架
        if self.config.getint("msg_frame", "enable")==1 and not self.init_msgframe():
            print "Failed to init msgframe"
            return -9
            '''

        # 注册运行模块
        if not self.register_modules():
            print "Failed to register modules"
            return -7

        # 载入全局数据
        if not self.read_frame_data():
            print "Failed to read frame data"
            return -8

        print "Success to initialize dataframe"
        LOG_NOTICE("Success to initialize dataframe")
        LOG_NOTICE("Register {0} modules to run".format(len(self.modules)))

        return 0

    def load_config(self, conf_file):
        """
            载入配置文件
        """
        self.config = ConfigParser.SafeConfigParser()
        suc_load_file = self.config.read(conf_file)
        if len(suc_load_file) == 0:
            print "There is no configure file to read"
            return False

        return True


    def init_log(self):
        """
            初始化日志信息
        """
        self.log_path = self.config.get('dataframe', 'log_path')
        self.log_file = self.config.get('dataframe', 'log_file')
        self.log_level = self.config.get('dataframe', 'log_level')

        init_main_log(self.name, self.log_level, self.log_path+os.sep+self.log_file)

        return True

    def set_status_info(self):
        """
            初始化状态信息
        """
        self.status_file_path = self.config.get('dataframe', 'status_file_path')
        if not os.path.exists(self.status_file_path):
            os.mkdir(self.status_file_path)

        status_file = self.status_file_path + os.sep + "{0}".format(self.name)
        if os.path.exists(status_file):
            LOG_WARNING("status file:{0} is exists, return".format(status_file))
            return False

        if not set_status_file(status_file):
            print "Failed to set status file: {0}".format(status_file)
            return False

        return True


    def remove_status_info(self):
        # 删除status文件
        status_file = self.status_file_path + os.sep + "{0}".format(self.name)
        remove_status_file(status_file)



    def set_time_info(self):
        """
            初始化时间信息
        """
        self.time_file_path = self.config.get('dataframe', 'time_file_path')
        if not os.path.exists(self.time_file_path):
            os.mkdir(self.time_file_path)

        time_file = self.time_file_path + os.sep + "{0}.time".format(self.name)
        set_time_file(time_file)

        return True


    def load_modules(self, module_file):
        """
            动态载入模块.
            输入： module的配置文件
            输出： 成功载入的模块数量
        """
        load_module_parser = LoadModuleParser()
        if not load_module_parser.parse(module_file):
            LOG_WARNING("Failed to parse load module file")
            return 0

        for (package, name) in load_module_parser.get_load_modules():
            LoadModule = importlib.import_module(package)
            LoadClass = getattr(LoadModule, name)
            module = LoadClass(name)
            self.modules.append(module)

        return len(self.modules)


    def register_modules(self):
        """
            注册需要运行的模块
        """
        # 载入模块
        load_module_file = self.config.get("modules", "load_module_conf")
        if not self.load_modules(load_module_file):
            return False

        # 模块初始化
        for module in self.modules:
            if not module.init(self):
                LOG_FATAL("Failed to initialize module: {0}".format(module.name))
                return False

        LOG_NOTICE("Success to register all the modules")
        return True

    def init_db(self):
        """
            初始化数据库信息
        """
        self.noveldb = FrameDB()
        db_conf_file = self.config.get('mysql', 'db_conf')
        self.noveldb.load_db(db_conf_file)

        return True

    def sync_data(self):
        """
            同步数据
        """

        self.client_sync = ClientSync()
        client_sync_conf_file = self.config.get('client_sync', 'sync_conf')
        self.client_sync.synchronization(client_sync_conf_file)

        return True

    def init_msgframe(self):
        """
            初始化消息框架
        """
        self.msg_frame = MsgFrame()
        msg_frame_conf_file = self.config.get('msg_frame', 'msg_frame_conf')
        self.msg_frame.load_conf(msg_frame_conf_file)
        self.msg_frame.init()
        return True

    def run(self):
        """
            运行注册的各个模块
        """
        for module in self.modules:
            if not module.run():
                LOG_WARNING("Module: {0} run FAILED".format(module.name))
                return False
            else:
                LOG_NOTICE("Module; {0} run SUCCESS".format(module.name))

        return True

    def join(self):
        """
            等待模块运行完
        """
        for m in self.modules:
            m.join()

    def stop(self):
        """
            强制终止模块运行
        """
        for m in self.modules:
            m.stop()

    def exit(self):
        """
            退出
        """
        # 退出各个模块
        for m in self.modules:
            m.exit()

        #self.remove_status_info()


    def read_frame_data(self):
        """
            读取框架运行时需要的全局数据
        """
        # 读取热门小说数据
        if not self.read_top_novels():
            LOG_WARNING("Failed to read top novels")
            return False

        return True


    def read_top_novels(self):
        """
            读取热门小说信息
        """
        top_novels_file = self.config.get("data", "top_novels_file")
        try:
            f = open(top_novels_file, "r")
            for line in f:
                book_name = line.rstrip('\n \t\r')
                self.top_novels.setdefault(book_name, 1)
            f.close()
        except IOError as e:
            LOG_WARNING("Failed to open top novels file:{0}".format(top_novels_file))
            return False

        LOG_NOTICE("Success to read {0} novels to process".format(len(self.top_novels)))

        return True


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
        exit()

    # 初始化
    frame = MainFrame()
    try:
        error = 0
        error = frame.init(sys.argv[1])
        if  error != 0:
            raise Exception
    except Exception as e:

        if error == -3:
            LOG_WARNING("exception:{0}".format(traceback.format_exc()))
        else:
            LOG_FATAL("exception:{0}".format(traceback.format_exc()))

        #frame.remove_status_info()
        sys.exit("Failed to init frame, exception:{0}".format(traceback.format_exc()))
    else:
        LOG_TRACE('MainFrame init success')

    # 注册信号量
    def sig_hander(signum, sigframe):
        frame.stop()
    signal.signal(signal.SIGINT, sig_hander)
    signal.signal(signal.SIGTERM, sig_hander)

    # 执行逻辑模块
    try:
        frame.run()
    except Exception as e:
        LOG_FATAL("exception:{0}".format(traceback.format_exc()))
        #frame.remove_status_info()
        sys.exit("Failed to run frame, exception:{0}".format(traceback.format_exc()))
    else:
        LOG_NOTICE('MainFrame run over')

    # 执行完所有模块，退出
    try:
        frame.join()
        frame.exit()
    except Exception as e:
        LOG_FATAL("exception:{0}".format(traceback.format_exc()))
        sys.exit("Failed to exit frame, exception:{0}".format(traceback.format_exc()))
    else:
        LOG_NOTICE('MainFrame exit')

    subprocess.call('./bin/dataframe_control stop', shell=True)
