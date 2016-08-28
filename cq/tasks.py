from django.utils import timezone
from django.core.cache import cache
from django.core.management import call_command

from .backends import backend
from .decorators import task
from .models import Task


@task
def clean_up(task, *args):
    """Remove stale tasks.

    Removes tasks that succeeded that are a week or older. Also
    removes any task older than a month. Runs once per day at midnight.
    """
    now = timezone.now()
    to_del = Task.objects.filter(
        status=Task.STATUS_SUCCESS,
        result_expiry__lte=now
    )
    if len(to_del):
        task.log('Cleaned up: {}'.format(', '.join([str(o.id) for o in to_del])))
        to_del.delete()


@task
def retry_tasks(cqtask, *args):
    retry = Task.objects.filter(status=Task.STATUS_RETRY)[:20]  # Cap at 20
    for task in retry:
        cqtask.log('Retrying: {}'.format(task.id))
        task.retry()


@task
def check_lost(cqtask, *args):
    running_task_ids = backend.get_running_tasks()
    cqtask.log('Running tasks: {}'.format(running_task_ids))
    queued_task_ids = backend.get_queued_tasks()
    cqtask.log('Queued tasks: {}'.format(queued_task_ids))
    queued_tasks = Task.objects.filter(status=Task.STATUS_QUEUED)
    running_tasks = Task.objects.filter(status=Task.STATUS_RUNNING)
    for task in queued_tasks:
        if task.id not in queued_task_ids:
            with cache.lock(str(task.id)):
                if task.at_risk == Task.AT_RISK_QUEUED:
                    cqtask.log('Lost in queue: {}'.format(task.id))
                    task.status = Task.STATUS_RETRY
                    task.save(update_fields=['status'])
                else:
                    task.at_risk = Task.AT_RISK_QUEUED
                    task.save(update_fields=['at_risk'])
    for task in running_tasks:
        if task.id not in running_task_ids:
            with cache.lock(str(task.id)):
                if task.at_risk == Task.AT_RISK_RUNNING:
                    cqtask.log('Lost on worker: {}'.format(task.id))
                    task.status = Task.STATUS_LOST
                    task.save(update_fields=['status'])
                else:
                    task.at_risk = Task.AT_RISK_RUNNING
                    task.save(update_fields=['at_risk'])


@task
def maintenance(task):
    retry_tasks()
    check_lost()
    clean_up()


@task
def call_command_task(task, *args, **kwargs):
    """A wrapper to call management commands.
    """
    return call_command(*args, **kwargs)