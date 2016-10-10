from datetime import timedelta
import logging

from django.utils import timezone
from django.core.cache import cache
from django.core.management import call_command

from .backends import backend
from .decorators import task
from .models import Task


@task
def clean_up(task, *args):
    """Remove stale tasks.

    Only remove tasks that have succeeded, are older than the TTl, have
    no dependencies that are still incomplete.
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
def retry_tasks(cqtask, *args, **kwargs):
    retry_delay = kwargs.pop('retry_delay', 1)
    retry = Task.objects.filter(status=Task.STATUS_RETRY)
    launched = 0
    for task in retry:
        next_retry = (task.retries ** 2) * timedelta(minutes=retry_delay)
        now = timezone.now()
        if not task.last_retry or (now - task.last_retry) >= next_retry:
            cqtask.log('Retrying: {}'.format(task.id))
            task.retry()
            launched += 1
            if launched >= 20:  # cap at 20
                break


@task
def check_lost(cqtask, *args):
    running_task_ids = backend.get_running_tasks()
    cqtask.log('Running tasks: {}'.format(running_task_ids), logging.DEBUG)
    queued_task_ids = backend.get_queued_tasks()
    cqtask.log('Queued tasks: {}'.format(queued_task_ids), logging.DEBUG)
    queued_tasks = Task.objects.filter(status=Task.STATUS_QUEUED)
    running_tasks = Task.objects.filter(status=Task.STATUS_RUNNING)
    for task in queued_tasks:
        if str(task.id) not in queued_task_ids:
            with cache.lock(str(task.id), timeout=2):
                if task.at_risk == Task.AT_RISK_QUEUED:
                    cqtask.log('Lost in queue: {}'.format(task.id))
                    task.status = Task.STATUS_LOST
                    task.save(update_fields=['status'])
                else:
                    task.at_risk = Task.AT_RISK_QUEUED
                    task.save(update_fields=['at_risk'])
    for task in running_tasks:
        if str(task.id) not in running_task_ids:
            with cache.lock(str(task.id), timeout=2):
                if task.at_risk == Task.AT_RISK_RUNNING:
                    cqtask.log('Lost on worker: {}'.format(task.id))
                    task.status = Task.STATUS_LOST
                    task.save(update_fields=['status'])
                else:
                    task.at_risk = Task.AT_RISK_RUNNING
                    task.save(update_fields=['at_risk'])


@task
def maintenance(task):
    retry_tasks(task=task)
    check_lost(task=task)
    clean_up(task=task)


@task
def call_command_task(task, *args, **kwargs):
    """A wrapper to call management commands.
    """
    return call_command(*args, **kwargs)


@task
def memory_details(task, method=None):
    if method == 'pympler':
        from pympler import muppy, summary
        all_objs = muppy.get_objects()
        summary.print_(summary.summarize(all_objs))
    elif method == 'mem_top':
        from mem_top import mem_top
        task.log(mem_top())
    else:
        import subprocess
        import shlex
        result = subprocess.check_output(
            'ps --no-headers -eo pmem,vsize,rss,pid,cmd | sort -k 1 -nr',
            shell=True
        )
        task.log('\n' + result.decode('utf8'))
