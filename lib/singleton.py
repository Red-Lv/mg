#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/03/25 14:19
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import sys


class Singleton(type):

    def __call__(cls, *args, **kwargs):

        if '_instance' not in vars(cls):
            cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            def skip_init(self, *args, **kwargs):
                pass
            cls.__init__ = skip_init

        return cls._instance


def main():
    class DemoSingleton(object):

        __metaclass__ = Singleton

        def __init__(self, s):
            self.s = s

    s1 = DemoSingleton('s1')
    s2 = DemoSingleton('s2')

    print id(s1), s1, vars(s1)
    print id(s2), s2, vars(s2)

if __name__ == '__main__':
    ret = main()
    sys.exit(ret)
