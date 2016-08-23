from functools import wraps

from django.core.management import call_command
from django.conf import settings

from .task import delay, SerialTask


__all__ = ('task', 'delay')


class _task(object):
    def __init__(self):
        pass

    def __call__(self, func):
        @wraps(func)
        def _delay(*args, **kwargs):
            if getattr(settings, 'CQ_SERIAL', False):
                # Create a SerialTask here to make sure we end up
                # returning the task instead of the result.
                kwargs['task'] = SerialTask()
                return self.wrapper(func, args, kwargs)
            else:
                return delay(func, args, kwargs)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.wrapper(func, args, kwargs)

        wrapper.delay = _delay
        return wrapper

    def wrapper(self, func, args, kwargs):
        task = kwargs.pop('task', None)
        direct = task is None
        serial = isinstance(task, SerialTask)
        if direct or serial:
            task = SerialTask()
        result = func(task, *args, **kwargs)
        if direct or serial:
            while isinstance(result, SerialTask):
                result = result.result
            task.result = result
            if direct:
                return task.result
            else:
                return task
        else:
            return result


def task(func):
    return _task()(func)


@task
def call_command_task(task, *args, **kwargs):
    """A wrapper to call management commands.
    """
    return call_command(*args, **kwargs)
