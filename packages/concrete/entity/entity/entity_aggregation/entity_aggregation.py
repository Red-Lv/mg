#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/11/01 21:18
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import Queue
import threading
import threadpool

from lib.globals import *

from packages.abstract.rabbitmq_client import *
from packages.concrete.entity.entity.entity_aggregation.entity_aggregation_toolkit import *


class EntityAggregation(RabbitMQClient):
    """Entity aggregation module
    """

    def __init__(self):

        RabbitMQClient.__init__(self)

        self.msg_to_publish = Queue.Queue()
        self.entity_aggregation_toolkit = EntityAggregationToolkit(self.msg_to_publish)

        self.pool_size = 20
        self.thread_pool = None

    def init(self, config_path=None):

        RabbitMQClient.init(self, config_path=config_path)

        self.entity_aggregation_toolkit.init(self.config['entity_aggregation_toolkit'])

        self.pool_size = int(self.config['entity_aggregation'].get('pool_size', self.pool_size))
        self.thread_pool = threadpool.ThreadPool(self.pool_size)

        self.publish_thread = threading.Thread(target=self.publish)

        return True

    def __del__(self):

        RabbitMQClient.__del__(self)

    def exit(self):

        self._dismissed.set()

        RabbitMQClient.exit(self)

        self.publish_thread.join()

        return True

    def callback(self, ch, method, properties, body):

        LOG_DEBUG('msg received: [body: %s]', body)

        request = threadpool.makeRequests(self.entity_aggregation_toolkit.aggregate_material_to_entity, (body,))[0]
        self.thread_pool.putRequest(request)

        ch.basic_ack(delivery_tag=method.delivery_tag)

        return True

    def publish(self):
        """
        """

        while True:

            if self._dismissed.is_set():
                break

            routing_key, msg = self.msg_to_publish.get()
            RabbitMQClient.publish(self, routing_key=routing_key, body=msg)

            LOG_DEBUG('msg published: [routing_key: %s, body: %s]', routing_key, msg)

        return True

    def consume(self):
        """
        """

        RabbitMQClient.consume(self)

    def run(self):
        """
        """

        # invoke the publish thread
        self.publish_thread.start()

        # invoke the consume thread
        self.consume()

        return True

if __name__ == '__main__':

    entity_aggregation = EntityAggregation()
    entity_aggregation.init('./conf/entity_aggregation.conf')

    entity_aggregation.run()

