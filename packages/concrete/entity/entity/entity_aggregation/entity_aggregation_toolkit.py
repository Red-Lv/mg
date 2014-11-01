#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/11/01 21:52
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import json
import time
import threadpool
import configobj

from lib.globals import *
from lib.singleton import *

from packages.abstract.abstract_module import *


class EntityAggregationToolkit(object):
    """
    """

    ME_RELATION_ONE_TO_ONE = 0
    ME_RELATION_MULTI_TO_ONE = 1

    __metaclass__ = Singleton

    def __init__(self, msg_to_publish=None):

        self.msg_to_publish = msg_to_publish

    def init(self, config=None):

        if not self.msg_to_publish:
            return False

        self.config = config

        self.init_db()

        return True

    def __del__(self):

        pass

    def exit(self):

        return True

    def init_db(self):

        self.entity_identity_db = frame.mongo_db.fetch_dbhandler('db_entity_identity')
        self.entity_agg_db = frame.mongo_db.fetch_dbhandler('db_entity_agg')

        return True

    def aggregate_material_to_entity(self, material):
        """
        """

        try:
            material= json.loads(material)
        except Exception as e:
            return False

        LOG_INFO('url: %s', material['url'])

        appid = material.get('appid')
        if appid is None:
            return False

        config = self.config['appid_{0}'.format(appid)]
        if not config:
            return False

        me_relation = int(config.get('em_relation', 0))
        if me_relation == self.ME_RELATION_MULTI_TO_ONE:
            return False

        unique_key = material.get('unique_key') or material.get('url')
        if not unique_key:
            return False

        eid = calc_eid(unique_key)
        material['eid'] = eid

        entity = self.fetch_entity_agg(appid, eid)

        entity_status = self.check_entity_status(entity, material)
        if entity_status != 0 and entity_status != 1:
            return False

        entity = material

        self.dump_entity_identity(appid, unique_key, eid)
        self.dump_entity_agg(entity)

        self.pack_msg(entity)

        return True

    def fetch_entity_agg(self, appid, eid):
        """
        """

        document = None
        if not self.entity_agg_db:
            LOG_WARNING('the db is None')
            return False

        spec = {'eid': eid}

        collection = self.entity_agg_db['entity_agg_{0}'.format(appid)]
        document = collection.find_one(spec)

        return document

    def check_entity_status(self, entity, material):
        """
        """

        if not entity:
            return 1
        
        appid = material['appid']
        config = self.config['appid_{0}'.format(appid)]

        field_mark_status = config.get('field_mark_status')
        if not field_mark_status:
            return 2

        value_from_entity = entity.get(field_mark_status)
        value_from_material = material.get(field_mark_status)

        status_update_func = getattr(self, config.get('status_update_func'), 'check_status_by_value_increment')
        if not callable(status_update_func):
            return 2

        # @TODO
        # the name of the field can be tuned.
        field_value_enumerated = config.get('field_value_enumerated', [])

        is_update = status_update_func(value_from_entity, value_from_material, field_value_enumerated)

        return 0

    def dump_entity_identity(self, appid, unique_key, eid):
        """
        """

        if not self.entity_identity_db:
            LOG_WARNING('the db is None')
            return False

        collection = self.entity_identity_db['entity_identity_{0}'.format(appid)]

        spec = {'unique_key': unique_key,
                'eid': eid}

        collection.update(spec, spec, upsert=True)

        return True

    def dump_entity_agg(self, entity):

        if not self.entity_agg_db:
            LOG_WARNING('the db is None')
            return False

        appid = entity['appid']
        eid = entity['eid']

        collection = self.entity_agg_db['entity_agg_{0}'.format(appid)]

        spec = {'eid': eid}

        collection.update(spec, entity, upsert=True)

        return True

    def pack_msg(self, entity):

        appid = entity['appid']
        config = self.config['appid_{0}'.format(appid)]

        msg_obj = {}
        for msg_field in config.get('msg_fields', entity.keys()):
            msg_obj[msg_field] = entity[msg_field]

        routing_key = config.get('routing_key', '')
        msg = json_to_str(msg_obj)

        self.msg_to_publish.put((routing_key, msg))

        return True

    def check_status_by_value_increment(self, value1, value2, **kwargs):
        """
        """

        try:
            if value2 <= value1:
                return False
        except Exception as e:
            return False

        return True

    def check_status_by_value_change(self, value1, value2, **kwargs):
        """
        """

        try:
            if value2 == value1:
                return False
        except Exception as e:
            return False

        return True

    def check_status_by_value_hit(self, value1, value2, **kwargs):
        """
        """

        try:
            if value2 == value1:
                return False
        except Exception as e:
            return False

        value_hit_list = kwargs.get('field_value_enumerated', [])
        for value in value_hit_list:
            if value2 == value:
                break
        else:
            return False

        return True
