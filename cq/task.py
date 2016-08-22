from .signature import to_signature
from .models import Task


class SerialTask(object):
    def subtask(self, func, args=(), kwargs={}):
        return func(*args, task=self, **kwargs)


def chain(func, args, kwargs, parent=None, previous=None):
    sig = to_signature(func, args, kwargs)
    task = Task.objects.create(signature=sig, parent=parent, previous=previous)
    return task


def delay(func, args, kwargs, parent=None):
    task = chain(func, args, kwargs, parent)
    task.submit()
    return task
