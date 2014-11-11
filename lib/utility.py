#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 00:06
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import json
import sign
import codecs
import hashlib
import platform
import requests

from binascii import crc32


def json_to_str(obj, obj_encoding='UTF-8', str_encoding='UTF-8'):

    try:
        return json.dumps(obj, encoding=obj_encoding, ensure_ascii=False).encode(str_encoding)
    except Exception as e:
        return ''


def read_file_content_iter(file_path='', encoding='UTF-8'):
    """
    """

    try:
        with codecs.open(file_path, encoding=encoding) as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                yield line
    except Exception as e:
        pass


def read_file_content(file_path='', encoding='UTF-8'):
    """
    """

    try:
        with codecs.open(file_path, encoding=encoding) as fp:
            return fp.read()
    except Exception as e:
        return ''


def read_web_content(url='', encoding=None):
    """
    """

    try:
        r = requests.get(url)
        if encoding:
            r.encoding = encoding
        if r.status_code != requests.codes.ok:
            return u''
        return r.text
    except Exception as e:
        return u''

    return u''


def fs64_sign(key):
    """
    """
    os = platform.system()
    result = None
    if os == "Linux":
        sign_result = sign.fs64(key)
        key1 = sign_result[1]
        key2 = sign_result[2]
        result = int(key1<<32) + key2
    elif os == "Windows":
        m = hashlib.md5()
        m.update(key)
        result = int(m.hexdigest(), 16) & 0xFFFFFFFFFFFFFFFF

    return result
