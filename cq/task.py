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


def to_class_name(cls):
    return '{0}.{1}'.format(cls.__module__, cls.__name__)


def from_class_name(name):
    return TaskFunc.get_task_func(sig['func_name'])


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

    def __init__(self, name=None):
        self.name = name

    def __call__(self, func):
        @wraps(func)
        def _delay(*args, **kwargs):
            if getattr(settings, 'CQ_SERIAL', False):
                # Create a SerialTask here to make sure we end up
                # returning the task instead of the result.
                kwargs['task'] = SerialTask()
                return self.wrapper(func, args, kwargs)
            else:
                from .models import delay
                return delay(func, args, kwargs)

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
        return wrapper

    def wrapper(self, func, args, kwargs):
        task = kwargs.pop('task', None)
        direct = task is None
        serial = isinstance(task, SerialTask)
        if direct or serial:
            task = SerialTask()
        result = func(task, *args, **kwargs)
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
        return cls.task_table[func_name].name or func_name


class SerialTask(object):
    """
    """
    def __init__(self, result=None):
        self.id = uuid.uuid4()
        self.result = result

    def subtask(self, func, args=(), kwargs={}):
        # Note: A serial task will automatically be created.
        return func(*args, task=self, **kwargs)

    def chain(self, func, args=(), kwargs={}):
        all_args = args
        if self.result is not None:
            all_args = (self.result,) + args
        # Note: A serial task will automatically be created.
        return func(*all_args, task=self, **kwargs)

    def log(self, msg):
        logger.info(msg)


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
