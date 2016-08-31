import six
import inspect
import importlib


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
