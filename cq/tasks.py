from datetime import timedelta

from django.core.management import call_command
from django.utils import timezone

from .decorators import task
from .models import Task
from .utils import redis_connection


@task
def clean_up(task, *args):
    """ Remove stale tasks.

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
def clear_logs(cqt):
    """ Remove all logs from REDIS.
    """
    with redis_connection() as con:
        for key in con.keys('cq:*:logs'):
            con.delete(key)


@task
def retry_tasks(cqtask, *args, **kwargs):
    retry_delay = kwargs.pop('retry_delay', 1)
    tasks = Task.objects.filter(status=Task.STATUS_RETRY)
    launched = 0
    for t in tasks:
        next_retry = (t.retries ** 2) * timedelta(minutes=retry_delay)
        now = timezone.now()
        if not t.last_retry or (now - t.last_retry) >= next_retry:
            cqtask.log('Retrying: {}'.format(t.id))
            t.retry()
            launched += 1
            if launched >= 20:  # cap at 20
                break


@task
def maintenance(task):
    retry_tasks(task=task)
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
        result = subprocess.check_output(
            'ps --no-headers -eo pmem,vsize,rss,pid,cmd | sort -k 1 -nr',
            shell=True
        )
        task.log('\n' + result.decode('utf8'))
