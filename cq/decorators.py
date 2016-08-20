from functools import wraps

from .task import delay, SerialTask


__all__ = ('task', 'delay')


class _task(object):
    def __init__(self):
        pass

    def __call__(self, func):
        @wraps(func)
        def _delay(*args, **kwargs):
            return delay(func, args, kwargs)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.wrapper(func, args, kwargs)

        wrapper.delay = _delay
        return wrapper

    def wrapper(self, func, args, kwargs):
        task = kwargs.pop('task', None)
        if task is None:
            task = SerialTask()
        return func(*args, task=task, **kwargs)


def task(func):
    return _task()(func)
