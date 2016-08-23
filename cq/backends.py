import random
import time
from datetime import timedelta
import logging
import threading

from channels import Channel
from django_redis import get_redis_connection
from django.utils import timezone
from django.db.utils import ProgrammingError

from .models import Task, RepeatingTask
from .utils import rlock


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
    conn = get_redis_connection()
    pipe = conn.pipeline()
    pipe.lrange('cq-current', 0, -1)
    pipe.delete('cq-current')
    results = pipe.execute()
    return set([x.decode() for x in results[0]])


def worker_publish_current(*args, **kwargs):
    global current_task
    max_its = kwargs.pop('max_its', None)
    sleep_time = kwargs.pop('sleep_time', 5)
    cur_it = 0
    while 1:
        if max_its is not None:
            if cur_it >= max_its:
                return
        task_id = current_task
        if task_id is None:
            continue
        conn = get_redis_connection()
        conn.lpush('cq-current', task_id)
        if max_its is not None:
            cur_it += 1
        time.sleep(sleep_time)


def perform_scheduling():
    with rlock('cq:scheduler:lock'):
        logger.debug('Checking for scheduled tasks.')
        now = timezone.now()
        try:
            rtasks = RepeatingTask.objects.filter(next_run__lte=now)
            for rt in rtasks:
                rt.submit()
        except ProgrammingError:
            logger.warning('CQ scheduler not running, DB is out of date.')


def scheduler(*args, **kwargs):
    while 1:
        conn = get_redis_connection()
        if conn.setnx('cq:scheduler', 'dummy'):
            conn.expire('cq:scheduler', 30)
            perform_scheduling()
        now = timezone.now()
        delay = ((now + timedelta(minutes=1)).replace(second=0, microsecond=0) - now).total_seconds()
        logger.debug('Waiting {} seconds for next schedule attempt.'.format(delay))
        time.sleep(delay)
