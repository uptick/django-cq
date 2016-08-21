import random
import time
import json
import logging

from channels import Channel
from django_redis import get_redis_connection


logger = logging.getLogger('cq')


def get_queued_tasks():
    channels = ['cq-tasks']
    cl = Channel('cq-tasks').channel_layer
    indexes = cl._receive_many_list_names(channels)
    if indexes is None:
        return []
    index = random.choice(list(indexes.keys()))
    list_names = indexes[index]
    random.shuffle(list_names)
    conn = cl.connection(index)
    capacity = cl.get_capacity('cq-tasks')
    messages = conn.lrange(list_names[0], 0, capacity)
    if not messages:
        return {}
    task_ids = set()
    results = conn.mget(messages)
    for content in results:
        if content is None:
            continue
        content = cl.deserialize(content)
        task_ids.add(content['task_id'])
    return task_ids


def worker_check_lost():
    conn = get_redis_connection()
    conn.publish('cq-checkin:trigger', '')
    


def worker_listener():
    while 1:
        try:
            conn = get_redis_connection()
        except err:
            logger.error(str(err))
            time.sleep(0.5)
            continue
        try:
            pubsub = conn.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(['cq-checkin:trigger'])
            for item in pubsub.listen():
                pubsub.publish('cq-checkin:response', task_id)
        except:
            pass
