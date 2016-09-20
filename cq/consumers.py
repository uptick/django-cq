import logging

from django.db import transaction

from .models import Task
from .task import SerialTask, TaskFunc
from .backends import backend


logger = logging.getLogger('cq')


def run_task(message):
    try:
        task_id = message['task_id']
    except (TypeError, KeyError):
        logger.error('Invalid CQ message.')
        return
    task = Task.objects.get(id=task_id)
    func_name = task.signature['func_name']
    if task.status == Task.STATUS_REVOKED:
        logger.info('Not running revoked task: {}'.format(func_name))
        return
    logger.info('Running task: {}'.format(func_name))
    backend.set_current_task(task_id)
    task.pre_start()
    task_func = TaskFunc.get_task(func_name)
    if task_func.atomic:
        with transaction.atomic():
            _do_run_task(task_func, task)
    else:
        _do_run_task(task_func, task)


def _do_run_task(task_func, task):
    try:
        result = task.start(pre_start=False)
    except Exception as err:
        handle_failure(task_func, task, err)
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


def handle_failure(task_func, task, err):
    """Decide whether to retry a failed task.
    """
    if task.retries >= task_func.retries or not task_func.match_exceptions(err):
        task.failure(err)
    else:
        task.failure(err, retry=True)
