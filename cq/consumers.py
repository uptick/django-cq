import logging

from django.db import transaction

from .models import Task
from .task import SerialTask
from .backends import backend


logger = logging.getLogger('cq')


def run_task(message):
    task_id = message['task_id']
    task = Task.objects.get(id=task_id)
    logger.info('Running task: {}'.format(task.signature['func_name']))
    backend.set_current_task(task_id)
    task.pre_start()
    with transaction.atomic():
        try:
            result = task.start(pre_start=False)
        except Exception as err:
            task.failure(err)
        else:
            if isinstance(result, Task):
                task.waiting(task=result)
            else:
                if isinstance(result, SerialTask):
                    result = result.result
                if task.subtasks.exists():
                    task.waiting(result=result)
                else:
                    task.success(result)
        finally:
            backend.set_current_task()
