from django.utils import timezone
from django.core.cache import cache

from .backends import get_queued_tasks, get_running_tasks
from .decorators import task
from .models import Task


@task
def clean_up(task):
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
def retry_tasks(cqtask):
    retry = Task.objects.filter(status=Task.STATUS_RETRY)[:20]  # Cap at 20
    for task in retry:
        cqtask.log('Retrying: {}'.format(task.id))
        task.retry()


@task
def check_lost(cqtask):
    running_task_ids = get_running_tasks()
    queued_task_ids = get_queued_tasks()
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
