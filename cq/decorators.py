from .task import TaskFunc


def task(*args, **kwargs):
    if len(args):
        func_or_name = args[0]
        args = args[1:]
    else:
        func_or_name = None
    if callable(func_or_name):
        func = func_or_name
        name = None
    else:
        func = None
        name = func_or_name
    dec = TaskFunc(name, *args, **kwargs)
    if func:
        return dec(func)
    else:
        return dec
