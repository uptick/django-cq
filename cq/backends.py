import threading
import random
import time
import logging

from channels import Channel
from redis.exceptions import RedisError

from .utils import redis_connection


logger = logging.getLogger('cq')


class Backend(object):
    current_tasks = {}

    @classmethod
    def set_current_task(cls, task_id=None):
        with threading.Lock():
            t = threading.current_thread()
            if task_id is not None:
                cls.current_tasks[t] = task_id
            else:
                try:
                    del cls.current_tasks[t]
                except KeyError:
                    pass

    @classmethod
    def get_queued_tasks(cls):
        channels = ['cq-tasks']
        cl = Channel('cq-tasks').channel_layer
        indexes = cl._receive_many_list_names(channels)
        if indexes is None:
            return []
        index = random.choice(list(indexes.keys()))
        list_names = indexes[index]
        random.shuffle(list_names)
        conn = cl.connection(index)
        messages = conn.lrange(list_names[0], 0, -1)
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

    @classmethod
    def get_running_tasks(cls):
        with redis_connection() as conn:
            pipe = conn.pipeline()
            pipe.lrange('cq:current', 0, -1)
            pipe.delete('cq:current')
            results = pipe.execute()
            return set([x.decode() for x in results[0]])

    @classmethod
    def publish_current(cls, *args, **kwargs):
        max_its = kwargs.pop('max_its', None)
        sleep_time = kwargs.pop('sleep_time', 30)
        cur_it = 0
        while 1:
            if max_its is not None:
                if cur_it >= max_its:
                    return
            task_ids = [x for x in cls.current_tasks.values()]
            if task_ids:
                logger.debug('Publishing tasks: {}'.format(task_ids))
                try:
                    with redis_connection() as conn:
                        conn.lpush('cq:current', *task_ids)
                except RedisError:
                    pass
                cls.clear_inactive()
            if max_its is not None:
                cur_it += 1
            time.sleep(sleep_time)

    @classmethod
    def clear_inactive(cls):
        to_del = [t for t in cls.current_tasks.keys()
                  if (t != threading.main_thread() and not t.is_alive())]
        for t in to_del:
            del cls.current_tasks[t]

    @classmethod
    def clear_current(cls):
        with redis_connection() as conn:
            conn.delete('cq:current')


backend = Backend()
