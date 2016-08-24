import six
import inspect

from .utils import import_attribute as from_func_name


def to_func_name(func):
    if inspect.isfunction(func) or inspect.isbuiltin(func):
        name = '{0}.{1}'.format(func.__module__, func.__name__)
    elif isinstance(func, six.string_types):
        name = str(func)
    else:
        msg = 'Expected a callable or a string, but got: {}'.format(func)
        raise TypeError(msg)
    return name


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
    func_name = sig['func_name']
    if func_name is None:
        return None
    func = from_func_name(func_name)
    return (func, sig.get('args', ()), sig.get('kwargs', {}))
