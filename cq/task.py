import inspect
from functools import wraps
import uuid
import logging

from django.conf import settings
from django import forms

from .utils import to_import_string, import_attribute


logger = logging.getLogger('cq')


def to_func_name(func):
    # Try to convert to a name before returing. Will default
    # to the import string.
    return TaskFunc.get_name(to_import_string(func))


def to_signature(func, args, kwargs):
    return {
        'func_name': to_func_name(func),
        'args': args,
        'kwargs': kwargs
    }


def from_signature(sig):
    func = TaskFunc.get_task_func(sig['func_name'])
    return (func, tuple(sig.get('args', ())), sig.get('kwargs', {}))


class TaskFunc(object):
    task_table = {}
    name_table = {}

    def __init__(self, name=None, atomic=True, **kwargs):
        self.name = name
        self.atomic = atomic
        self.retries = kwargs.get('retries', 0)
        self.retry_exceptions = kwargs.get('retry_exceptions', [])
        if not isinstance(self.retry_exceptions, (list, tuple)):
            self.retry_exceptions = [self.retry_exceptions]

    def __call__(self, func):
        @wraps(func)
        def _delay_args(args=(), kwargs={}, **kw):
            if getattr(settings, 'CQ_SERIAL', False):
                # Create a SerialTask here to make sure we end up
                # returning the task instead of the result.
                kwargs['task'] = SerialTask()
                return self.wrapper(func, args, kwargs, **kw)
            else:
                from .models import delay
                return delay(func, args, kwargs, **kw)

        @wraps(func)
        def _delay(*args, **kwargs):
            return _delay_args(args, kwargs)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.wrapper(func, args, kwargs)

        # Add the task to the tables.
        self.func = wrapper
        func_name = to_import_string(func)
        self.task_table[func_name] = self
        logger.debug('Adding task definition: {}'.format(func_name))
        if self.name and self.name != func_name:
            self.task_table[self.name] = self
            logger.debug('Adding task definition: {}'.format(self.name))
        if self.name:
            self.name_table[self.name] = func_name

        wrapper.delay = _delay
        wrapper.delay_args = _delay_args
        return wrapper

    def wrapper(self, func, args, kwargs):
        task = kwargs.pop('task', None)
        direct = task is None
        serial = isinstance(task, SerialTask)
        if direct or serial:
            task = SerialTask(parent=task, previous=task)
        try:
            result = func(task, *args, **kwargs)
        except Exception as err:
            if serial:
                for func, args, kwargs in task._errbacks:
                    func(*((task, err,) + tuple(args)), **kwargs)
            raise
        if direct or serial:
            while isinstance(result, SerialTask):
                result = result.result
            task.result = result
            if direct:
                return task.result
            else:
                return task
        else:
            return result

    @classmethod
    def get_task(cls, name):
        return cls.task_table[name]

    @classmethod
    def get_task_func(cls, name):
        try:
            return cls.get_task(name).func
        except KeyError:
            pass
        return import_attribute(name)

    @classmethod
    def get_name(cls, func_name):
        try:
            task = cls.task_table[func_name]
        except KeyError:
            return func_name
        return task.name or func_name

    def match_exceptions(self, error):
        def _is_exception(value):
            if not inspect.isclass(value):
                value = value.__class__
            return issubclass(value, Exception)

        def _is_instance(value, cls):
            if not inspect.isclass(cls):
                cls = cls.__class__
            return isinstance(value, cls)

        if not self.retry_exceptions:
            return True
        for ex in self.retry_exceptions:
            if callable(ex) and not _is_exception(ex) and ex(error):
                return True
            elif _is_instance(error, ex):
                return True
        return False


class SerialTask(object):
    """
    """
    def __init__(self, result=None, parent=None, previous=None):
        self.id = uuid.uuid4()
        self.result = result
        self.parent = parent
        self.previous = previous
        self._errbacks = []

    def subtask(self, func, args=(), kwargs={}, **kw):
        # Note: A serial task will automatically be created.
        return func(*args, task=self, **kwargs)

    def chain(self, func, args=(), kwargs={}, **kw):
        # Note: A serial task will automatically be created.
        return func(*args, task=self, **kwargs)

    def errorback(self, func, args=(), kwargs={}):
        self._errbacks.append((func, args, kwargs))

    def log(self, msg, level=logging.INFO, **kwargs):
        logger.log(level, msg)


class FuncNameWidget(forms.TextInput):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._name = 'func_name'
        self._list = sorted(list(TaskFunc.task_table.keys()))
        self.attrs.update({'list': 'list__%s' % self._name})

    def render(self, name, value, attrs=None):
        text_html = super().render(name, value, attrs=attrs)
        data_list = '<datalist id="list__%s">' % self._name
        for item in self._list:
            data_list += '<option value="%s">' % item
        data_list += '</datalist>'
        return (text_html + data_list)
