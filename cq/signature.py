import six
import inspect

from .utils import import_attribute


def to_signature(func, args, kwargs):
    sig = {}
    if inspect.isfunction(func) or inspect.isbuiltin(func):
        sig['func_name'] = '{0}.{1}'.format(func.__module__, func.__name__)
    elif isinstance(func, six.string_types):
        sig['func_name'] = str(func)
    else:
        msg = 'Expected a callable or a string, but got: {}'.format(func)
        raise TypeError(msg)
    sig['args'] = args
    sig['kwargs'] = kwargs
    return sig


def from_signature(sig):
    func_name = sig['func_name']
    if func_name is None:
        return None
    func = import_attribute(func_name)
    return (func, sig.get('args', ()), sig.get('kwargs', {}))
