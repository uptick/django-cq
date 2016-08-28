import six
import inspect

from .task import TaskFunc


def to_func_name(func):

    # Convert to a module import string.
    if inspect.isfunction(func) or inspect.isbuiltin(func):
        name = '{0}.{1}'.format(func.__module__, func.__name__)
    elif isinstance(func, six.string_types):
        name = str(func)
    else:
        msg = 'Expected a callable or a string, but got: {}'.format(func)
        raise TypeError(msg)

    # Try to convert to a name before returing. Will default
    # to the import string.
    return TaskFunc.get_name(name)


def to_class_name(cls):
    return '{0}.{1}'.format(cls.__module__, cls.__name__)


def from_class_name(name):
    return from_func_name(name)


def to_signature(func, args, kwargs):
    return {
        'func_name': to_func_name(func),
        'args': args,
        'kwargs': kwargs
    }


def from_signature(sig):
    func = TaskFunc.get_task(sig['func_name']).func
    return (func, tuple(sig.get('args', ())), sig.get('kwargs', {}))
