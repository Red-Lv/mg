#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/10/29 00:06
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import json


def json_to_str(obj, obj_encoding='UTF-8', str_encoding='UTF-8'):

    try:
        return json.dumps(obj, encoding=obj_encoding, ensure_ascii=False).encode(str_encoding)
    except Exception as e:
        return ''
