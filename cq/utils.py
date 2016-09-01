import time
from contextlib import contextmanager
import six
import inspect
import importlib
import logging

from redis.exceptions import RedisError
from django_redis import get_redis_connection


logger = logging.getLogger('cq')


def to_import_string(func):
    if inspect.isfunction(func) or inspect.isbuiltin(func):
        name = '{0}.{1}'.format(func.__module__, func.__name__)
    elif isinstance(func, six.string_types):
        name = str(func)
    elif inspect.isclass(func):
        return '{0}.{1}'.format(func.__module__, func.__name__)
    else:
        msg = 'Expected a callable or a string, but got: {}'.format(func)
        raise TypeError(msg)
    return name


def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func").
    """
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)


@contextmanager
def redis_connection(retries=3, sleep_time=0.5):
    while 1:
        try:
            conn = get_redis_connection()
            break
        except RedisError:
            if retries is None or retries == 0:
                raise
            retries -= 1
            time.sleep(sleep_time)
    try:
        yield conn
    finally:
        pass
        # This is actually not needed. The call to `get_redis_connection`
        # shares a single connection.
        # conn.release()
