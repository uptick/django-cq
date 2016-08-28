from .task import TaskFunc


def task(func_or_name, *args, **kwargs):
    if callable(func_or_name):
        func = func_or_name
        name = None
    else:
        func = None
        name = func_or_name
    dec = TaskFunc(name)
    if func:
        return dec(func)
    else:
        return dec
