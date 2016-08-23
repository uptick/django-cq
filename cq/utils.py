import contextlib
import importlib

from django.conf import settings
from redlock import Redlock, CannotObtainLock


lock_conn = Redlock([settings.REDIS_URL])


@contextlib.contextmanager
def rlock(resource, ttl=1000):
    lock = lock_conn.lock(resource, ttl)
    if not lock:
        raise CannotObtainLock('Timed out.')
    try:
        yield lock
    finally:
        lock_conn.unlock(lock)


def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func").
    """
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)



