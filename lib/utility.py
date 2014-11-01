#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 00:06
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import json
import requests


def json_to_str(obj, obj_encoding='UTF-8', str_encoding='UTF-8'):

    try:
        return json.dumps(obj, encoding=obj_encoding, ensure_ascii=False).encode(str_encoding)
    except Exception as e:
        return ''


def read_file_content_iter(file_path=''):
    """
    """

    try:
        with open(file_path) as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                yield line
    except Exception as e:
        pass


def read_file_content(file_path=''):
    """
    """

    try:
        with open(file_path) as fp:
            return fp.read()
    except Exception as e:
        return ''


def read_web_content(url=''):
    """
    """

    try:
        r = requests.get(url)
        if r.status_code != requests.codes.ok:
            return ''
        return r.text.encode('UTF-8')
    except Exception as e:
        return ''

    return ''
