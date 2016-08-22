from .signature import to_signature
from .models import Task


class SerialTask(object):
    def subtask(self, func, args=(), kwargs={}):
        return func(*args, task=self, **kwargs)


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
