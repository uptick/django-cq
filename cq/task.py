import uuid
import logging

from .signature import to_signature
from .models import Task


logger = logging.getLogger('cq')


class SerialTask(object):
    """
    """
    def __init__(self, result=None):
        self.id = uuid.uuid4()
        self.result = result

    def subtask(self, func, args=(), kwargs={}):
        # Note: A serial task will automatically be created.
        return func(*args, task=self, **kwargs)
        # task = SerialTask()
        # task.result = func(task, *args, **kwargs)
        # return task

    def chain(self, func, args=(), kwargs={}):
        all_args = args
        if self.result is not None:
            all_args = (self.result,) + args
        # Note: A serial task will automatically be created.
        return func(*all_args, task=self, **kwargs)
        # task = SerialTask()
        # task.result = func(task, *all_args, **kwargs)
        # return task

    def log(self, msg):
        logger.info(msg)


def chain(func, args, kwargs, parent=None, previous=None):
    """Run a task after an existing task.

    The result is passed as the first argument to the chained task.
    If no parent is specified, automatically use the parent of the
    predecessor. Note that I'm not sure this is the correct behavior,
    but is useful for making sure logs to where they should.
    """
    sig = to_signature(func, args, kwargs)
    if parent is None and previous:
        parent = previous.parent
    task = Task.objects.create(signature=sig, parent=parent, previous=previous)
    return task


def delay(func, args, kwargs, parent=None):
    task = chain(func, args, kwargs, parent)
    task.submit()
    return task
