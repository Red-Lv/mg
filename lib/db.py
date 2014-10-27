#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/27 16:20
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import random
import configobj

import MySQLdb
import pymongo
from pymongo.read_preferences import ReadPreference


from singleton import *


class AbstractDB(object):
    """Abstract db module
    """

    __metaclass__ = Singleton

    def __init__(self):

        self.dbhandler_dict = {}

        pass

    def init(self, *args, **kwargs):

        return True

    def __del__(self):

        pass

    def exit(self):

        return True


class MySqlDB(AbstractDB):
    """MySQL database collection frame
    """

    def __init__(self):

        AbstractDB.__init__(self)

    def init(self, config_path):

        AbstractDB.init(self, config_path)

        self.read_config(config_path)

        return True

    def __del__(self):

        AbstractDB.__del__(self)

        for conn in self.dbhandler_dict:
            self.push_back_dbhandler(conn)

    def exit(self):

        AbstractDB.exit(self)

        return True

    def read_config(self, config_path):
        """
        """

        self.config = configobj.ConfigObj(config_path)
        self.config = self.config['MySqlDB']

        return True

    def init_conn(self, conn):

        conn.set_character_set('GBK')
        conn.autocommit(True)

        return True

    def connect(self, db_cluster_key):
        """
        """

        conn = None
        if db_cluster_key not in self.config:
            return conn

        db_cluster_config = self.config[db_cluster_key]
        db_cluster_config_section = random.sample(db_cluster_config.sections, 1)[0]
        db_config = db_cluster_config[db_cluster_config_section]
        db_config['port'] = int(db_config['port'])

        try:
            conn = MySQLdb.connect(**db_config)
            self.init_conn(conn)
            self.dbhandler_dict[conn] = conn
        except Exception as e:
            conn = None

        return conn

    def fetch_dbhandler(self, db_cluster_key):
        """
        """

        return self.connect(db_cluster_key)

    def push_back_dbhandler(self, conn):

        try:
            if conn in self.dbhandler_dict:
                self.dbhandler_dict[conn].close()
        except Exception as e:
            pass

        return True


class MongoDB(AbstractDB):
    """Mongo database frame
    """

    def __init__(self):

        AbstractDB.__init__(self)

    def init(self, config_path):

        AbstractDB.init(self, config_path)

        self.read_config(config_path)

        return True

    def __del__(self):

        AbstractDB.__del__(self)

        for db in self.dbhandler_dict:
            self.push_back_dbhandler(db)

    def exit(self):

        AbstractDB.exit(self)

        return True

    def read_config(self, config_path):
        """
        """

        self.config = configobj.ConfigObj(config_path)
        self.config = self.config['MongoDB']

        return True

    def connect(self, db_cluster_key):
        """
        """

        db = None
        if db_cluster_key not in self.config:
            return db

        db_cluster_config = self.config[db_cluster_key]
        try:
            hosts_or_uri = db_cluster_config['hosts_or_uri']
            if hosts_or_uri.find('replicaSet') == -1:
                client = pymongo.MongoClient(hosts_or_uri)
            else:
                client = pymongo.MongoReplicaSetClient(hosts_or_uri)
                client.read_preference = ReadPreference.SECONDARY_PREFERRED

            db = client[db_cluster_config['db']]
            self.dbhandler_dict[db] = client
        except Exception as e:
            db = None

        return db

    def fetch_dbhandler(self, db_cluster_key):

        return self.connect(db_cluster_key)

    def push_back_dbhandler(self, db):
        """
        """

        try:
            if db in self.dbhandler_dict:
                self.dbhandler_dict[db].disconnect()
        except Exception as e:
            pass

        return True

if __name__ == '__main__':

    mysql_db = MySqlDB()
    mongo_db = MongoDB()

    mysql_db.init('../conf/db.conf')
    mongo_db.init('../conf/db.conf')

    db = mongo_db.fetch_dbhandler('db_test')
    for document in db.testcollection.find():
        print document

    mongo_db.push_back_dbhandler(db)

    print '*' * 20

    conn = mysql_db.fetch_dbhandler('db_ori_even')
    cursor = conn.cursor()
    cursor.execute('Select * from dir_ori_info0 LIMIT 100')
    rows = cursor.fetchall()
    for row in rows:
        print row

    cursor.close()
    mysql_db.push_back_dbhandler(conn)
