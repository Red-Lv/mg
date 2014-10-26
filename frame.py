#!/usr/bin/env python
# -*- coding:GBK

'''
@module:MainFrame
@date: 2012-8-20
@author: zhangjianrong@baidu.com
@brief: �������������
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
         ��������
         1�������ѡģ��modulesΪ�� �� ���� conf_path �е�conf ����������
         2����Σ������ѡģ��Ƿ����򷵻ش�����Ϣ��ִֹͣ��
         3���ٴΣ�ִ�п�ѡģ��
      '''
    print msg

class MainFrame(object):
    """
        ģ��������
    """
    def __init__(self):
        self.name = None
        self.config = None                 	# ȫ�������ļ����
        self.modules = []					# ����ģ�鼯��
        self.noveldb = None					# NovelDB���
        self.top_novels = {}                # ����С˵
        self.status_file_path = ""          # ״̬�ļ�Ŀ¼
        self.status_file = ""               # ״̬�ļ�
        self.time_file_path = ""            # ʱ���ļ�Ŀ¼
        self.msg_frame = None               # ��Ϣ���


    def init(self, confFile):
        """
            ��ܳ�ʼ������
        """
        # ���������ļ���Ϣ
        if not self.load_config(confFile):
            print "Failed to load configure file"
            return -1

        # ���ÿ�ܵ�name����������ʱ�䣬״̬��Ϣ
        self.name = self.config.get("dataframe", "name")
        if len(self.name) == 0:
            print "There is no name for dataframe"
            return -1

        # ��ʼ����־ģ��
        if not self.init_log():
            print "Failed to initialize log info"
            return -2

        # ��ʼ��״̬��Ϣ
        '''
        if not self.set_status_info():
            print "Failed to initialize status info"
            return -3
            '''

        # ��ʼ��ʱ����Ϣ
        '''
        if not self.set_time_info():
            print "Failed to initialize time info"
            return -4
            '''

        # ��ʼ�����ݿ�
        if not self.init_db():
            print "Failed to initialize database"
            return -5

        if not self.sync_data():
            print 'Failed to sync data'
            return -6

        '''
        # ��ʼ����Ϣ���
        if self.config.getint("msg_frame", "enable")==1 and not self.init_msgframe():
            print "Failed to init msgframe"
            return -9
            '''

        # ע������ģ��
        if not self.register_modules():
            print "Failed to register modules"
            return -7

        # ����ȫ������
        if not self.read_frame_data():
            print "Failed to read frame data"
            return -8

        print "Success to initialize dataframe"
        LOG_NOTICE("Success to initialize dataframe")
        LOG_NOTICE("Register {0} modules to run".format(len(self.modules)))

        return 0

    def load_config(self, conf_file):
        """
            ���������ļ�
        """
        self.config = ConfigParser.SafeConfigParser()
        suc_load_file = self.config.read(conf_file)
        if len(suc_load_file) == 0:
            print "There is no configure file to read"
            return False

        return True


    def init_log(self):
        """
            ��ʼ����־��Ϣ
        """
        self.log_path = self.config.get('dataframe', 'log_path')
        self.log_file = self.config.get('dataframe', 'log_file')
        self.log_level = self.config.get('dataframe', 'log_level')

        init_main_log(self.name, self.log_level, self.log_path+os.sep+self.log_file)

        return True

    def set_status_info(self):
        """
            ��ʼ��״̬��Ϣ
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
        # ɾ��status�ļ�
        status_file = self.status_file_path + os.sep + "{0}".format(self.name)
        remove_status_file(status_file)



    def set_time_info(self):
        """
            ��ʼ��ʱ����Ϣ
        """
        self.time_file_path = self.config.get('dataframe', 'time_file_path')
        if not os.path.exists(self.time_file_path):
            os.mkdir(self.time_file_path)

        time_file = self.time_file_path + os.sep + "{0}.time".format(self.name)
        set_time_file(time_file)

        return True


    def load_modules(self, module_file):
        """
            ��̬����ģ��.
            ���룺 module�������ļ�
            ����� �ɹ������ģ������
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
            ע����Ҫ���е�ģ��
        """
        # ����ģ��
        load_module_file = self.config.get("modules", "load_module_conf")
        if not self.load_modules(load_module_file):
            return False

        # ģ���ʼ��
        for module in self.modules:
            if not module.init(self):
                LOG_FATAL("Failed to initialize module: {0}".format(module.name))
                return False

        LOG_NOTICE("Success to register all the modules")
        return True

    def init_db(self):
        """
            ��ʼ�����ݿ���Ϣ
        """
        self.noveldb = FrameDB()
        db_conf_file = self.config.get('mysql', 'db_conf')
        self.noveldb.load_db(db_conf_file)

        return True

    def sync_data(self):
        """
            ͬ������
        """

        self.client_sync = ClientSync()
        client_sync_conf_file = self.config.get('client_sync', 'sync_conf')
        self.client_sync.synchronization(client_sync_conf_file)

        return True

    def init_msgframe(self):
        """
            ��ʼ����Ϣ���
        """
        self.msg_frame = MsgFrame()
        msg_frame_conf_file = self.config.get('msg_frame', 'msg_frame_conf')
        self.msg_frame.load_conf(msg_frame_conf_file)
        self.msg_frame.init()
        return True

    def run(self):
        """
            ����ע��ĸ���ģ��
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
            �ȴ�ģ��������
        """
        for m in self.modules:
            m.join()

    def stop(self):
        """
            ǿ����ֹģ������
        """
        for m in self.modules:
            m.stop()

    def exit(self):
        """
            �˳�
        """
        # �˳�����ģ��
        for m in self.modules:
            m.exit()

        #self.remove_status_info()


    def read_frame_data(self):
        """
            ��ȡ�������ʱ��Ҫ��ȫ������
        """
        # ��ȡ����С˵����
        if not self.read_top_novels():
            LOG_WARNING("Failed to read top novels")
            return False

        return True


    def read_top_novels(self):
        """
            ��ȡ����С˵��Ϣ
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

    # ��ʼ��
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

    # ע���ź���
    def sig_hander(signum, sigframe):
        frame.stop()
    signal.signal(signal.SIGINT, sig_hander)
    signal.signal(signal.SIGTERM, sig_hander)

    # ִ���߼�ģ��
    try:
        frame.run()
    except Exception as e:
        LOG_FATAL("exception:{0}".format(traceback.format_exc()))
        #frame.remove_status_info()
        sys.exit("Failed to run frame, exception:{0}".format(traceback.format_exc()))
    else:
        LOG_NOTICE('MainFrame run over')

    # ִ��������ģ�飬�˳�
    try:
        frame.join()
        frame.exit()
    except Exception as e:
        LOG_FATAL("exception:{0}".format(traceback.format_exc()))
        sys.exit("Failed to exit frame, exception:{0}".format(traceback.format_exc()))
    else:
        LOG_NOTICE('MainFrame exit')

    subprocess.call('./bin/dataframe_control stop', shell=True)
