import importlib


def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func").
    """
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)
