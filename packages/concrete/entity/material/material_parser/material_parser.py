#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/11/01 15:35
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import time
import xml.etree.ElementTree as ET

from lib.globals import *
from packages.abstract.rabbitmq_client import *


class MaterialParser(RabbitMQClient):
    """
    """

    MATERIAL_FROM_UNIVERSE = 0
    MATERIAL_FROM_FILE = 1
    MATERIAL_FROM_WEB = 2
    MATERIAL_FROM_MSG = 3

    def __init__(self):

        RabbitMQClient.__init__(self)

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path=config_path)

        ret = self.init_material_parser()
        if not ret:
            return False

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        RabbitMQClient.exit(self)

        return True

    def init_material_parser(self):
        """Initialize material parser from config file
        """

        self.material_source = self.MATERIAL_FROM_UNIVERSE
        config = self.config.get('material_parser')
        if not config:
            LOG_WARNING('fail to initialize material parser. error: %s', 'material parser config does not exist')
            return False

        self.appid = int(config.get('appid', 0))
        if not self.appid:
            LOG_WARNING('appid is undefined')
            return False

        material_source_config = config.get('material_from_file')
        if material_source_config:
            is_enable = int(material_source_config.get('is_enable', '0'))
            if is_enable:
                self.material_source = self.MATERIAL_FROM_FILE
                self.material_source_file = material_source_config.get('material_source_file', '')
                self.material_parser = material_source_config.get('material_parser', '')

        if self.material_source:
            LOG_INFO('success in initializing config parser. material_source: %s', self.material_source)
            return True

        material_source_config = config.get('material_from_web')
        if material_source_config:
            is_enable = int(material_source_config.get('is_enable', '0'))
            if is_enable:
                self.material_source = self.MATERIAL_FROM_WEB
                self.material_source_file = material_source_config.get('material_source_file', '')
                self.material_parser = material_source_config.get('material_parser', '')

        if self.material_source:
            LOG_INFO('success in initializing config parser. material_source: %s', self.material_source)
            return True

        material_source_config = config.get('material_from_msg')
        if material_source_config:
            is_enable = int(material_source_config.get('is_enable', '0'))
            if is_enable:
                self.material_source = self.MATERIAL_FROM_WEB
                # @TODO
                # init a thread to consumer message from the special socket

        if self.material_source:
            LOG_INFO('success in initializing config parser. material_source: %s', self.material_source)
            return True

        LOG_WARNING('fail to initialize material parser. error: %s', 'material source is not configured')

        return False

    def run(self):
        """
        """

        if not self.material_source:
            LOG_WARNING('fail to run material parser. error: %s', 'material source is undefined')
            return False

        LOG_INFO('start running material parser. material_source: %s', self.material_source)
        timestamp_s = int(time.time())

        if self.material_source == self.MATERIAL_FROM_FILE:
            self.parse_material_from_file()
        elif self.material_source == self.MATERIAL_FROM_WEB:
            self.parse_material_from_web()
        else:
            # @TODO
            pass

        timestamp_e = int(time.time())
        time_cost = timestamp_e - timestamp_s

        LOG_INFO('finish running material parser. time_cost: %s', time_cost)

        return True

    def parse_material_from_file(self):
        """
        """

        if not self.material_source_file:
            LOG_WARNING('fail to parse material from file. error: %s', 'material_source_file is undefined')
            return False

        for line in read_file_content_iter(self.material_source_file):

            LOG_INFO('start parsing material from file. file_path: %s', line)

            material = read_file_content(line)
            if not material:
                continue

            self.parse_material(_parser=self.material_parser, material=material)

        return True

    def parse_material_from_web(self):
        """
        """

        if not self.material_source_file:
            LOG_WARNING('fail to parse material from web. error: %s', 'material_source_file is undefined')
            return False

        for line in read_file_content_iter(self.material_source_file):

            LOG_INFO('start parsing material from web. url: %s', line)

            material = read_web_content(line, encoding='UTF-8')
            if not material:
                continue

            self.parse_material(_parser=self.material_parser, material=material)

        return True

    def parse_material(self, _parser=None, material=u''):
        """
        """

        parser = getattr(self, _parser, None)
        if not parser or not callable(parser):
            LOG_WARNING('parser is None or not callable. parser: %s', _parser)
            return False

        parser(material)

        return True

    def parse_material_comic(self, material=u''):
        """
        """

        if not material:
            return False

        LOG_INFO('start parsing material comic.')
        timestamp_s = int(time.time())

        try:
            root = ET.fromstring(material.encode('UTF-8'))
        except Exception as e:
            LOG_WARNING('fail to construct element tree from material. error: %s', e)
            return False

        def parse_material_comic_item(root):

            material_data = {}

            root = root.find('display')
            if root is None:
                return material_data

            material_data['appid'] = self.appid

            material_data['site_url'] = getattr(root.find('siteurl'), 'text', '').lower()
            material_data['site_name'] = getattr(root.find('sitename'), 'text', '')

            material_data['url'] = getattr(root.find('url'), 'text', '').lower()
            material_data['title'] = getattr(root.find('title'), 'text', '')
            material_data['formal_title'] = getattr(root.find('formal_title'), 'text', '')
            material_data['author'] = getattr(root.find('author'), 'text', '')
            material_data['logo'] = getattr(root.find('logo'), 'text', '').lower()
            material_data['status'] = getattr(root.find('status'), 'text', '')

            last_content = root.find('icon')
            if last_content is not None:
                material_data['last_content_url'] = getattr(last_content.find('iconlink'), 'text', '').lower()
                material_data['last_content_title'] = getattr(last_content.find('iconcontent'), 'text', '')
            else:
                material_data['last_content_url'] = ''
                material_data['last_content_title'] = ''
            
            last_content = root.findall('link')[-1]
            if material_data['last_content_url'] != getattr(last_content.find('linkurl'), 'text', '').lower():
                material_data['last_content_url'] = getattr(last_content.find('linkurl'), 'text', '').lower()
                material_data['last_content_title'] = getattr(last_content.find('linkcontent'), 'text', '')
                
            material_data['last_content_index'] = len((root.findall('link')))
            material_data['last_content_update_time'] = int(getattr(root.find('update_time'), 'text', int(time.time())))

            # @TODO
            # material validity check
            if not material_data['formal_title'] or not material_data['url'] or not material_data['last_content_url']:
                material_data = {}

            return material_data

        no_of_element = 0
        if root.tag != 'item':
            for child in root.findall('item'):
                material_data = parse_material_comic_item(child)
                if material_data:
                    RabbitMQClient.publish(self, body=json_to_str(material_data))
                    no_of_element += 1
        else:
            material_data = parse_material_comic_item(child)
            if material_data:
                RabbitMQClient.publish(self, body=json_to_str(material_data))
                no_of_element += 1

        timestamp_e = int(time.time())
        time_cost = timestamp_e - timestamp_s

        LOG_INFO('finish parsing material comic. no_of_element: %s, time_cost: %s.', no_of_element, time_cost)

        return True

if __name__ == '__main__':

    material_parser = MaterialParser()
    material_parser.init('./conf/material_parser.conf')

    material_parser.run()
