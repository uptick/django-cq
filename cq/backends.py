import random
# from datetime import datetime
import time
import json
import logging

from channels import Channel
from redis import Redis
from django.conf import settings

from .models import Task


logger = logging.getLogger('cq')


# For now we can use a global variable to store the
# current task. However, if threading is ever enabled
# on channel workers we'll need to reconsider.
current_task = None


def set_current_task(task_id=None):
    global current_task
    current_task = task_id


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
    # capacity = cl.get_capacity('cq-tasks')
    messages = conn.lrange(list_names[0], 0, -1)  # capacity)
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


def get_running_tasks():
    conn = Redis.from_url(settings.REDIS_URL)
    pipe = conn.pipeline()
    pipe.lrange('cq-current', 0, -1)
    pipe.delete('cq-current')
    results = pipe.execute()
    return set([x.decode() for x in results[0]])


def worker_check_lost():
    running_task_ids = get_running_tasks()
    queued_task_ids = get_queued_tasks()
    queued_tasks = Task.objects.filter(status=Task.STATUS_QUEUED)
    running_tasks = Task.objects.filter(status=Task.STATUS_RUNNING)
    for task in queued_tasks:
        if task.id not in queued_task_ids:
            if task.at_risk == Task.AT_RISK_QUEUED:
                task.status = Task.STATUS_LOST
                task.save(update_fields=['status'])
            else:
                task.at_risk = Task.AT_RISK_QUEUED
                task.save(update_fields=['at_risk'])
    for task in running_tasks:
        if task.id not in running_task_ids:
            if task.at_risk == Task.AT_RISK_RUNNING:
                task.status = Task.STATUS_LOST
                task.save(update_fields=['status'])
            else:
                task.at_risk = Task.AT_RISK_RUNNING
                task.save(update_fields=['at_risk'])


def worker_publish_current(*args, **kwargs):
    global current_task
    max_its = kwargs.pop('max_its', None)
    sleep_time = kwargs.pop('sleep_time', 5)
    cur_it = 0
    while 1:
        try:
            conn = Redis.from_url(settings.REDIS_URL)
        except Exception as err:
            logger.error(str(err))
            time.sleep(0.5)
            continue
        # try:
        while 1:
            if max_its is not None:
                if cur_it >= max_its:
                    return
            time.sleep(sleep_time)
            task_id = current_task
            if task_id is None:
                continue
            conn.lpush('cq-current', task_id)
            if max_its is not None:
                cur_it += 1
        # except:
        #     pass
