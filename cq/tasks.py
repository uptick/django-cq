from datetime import timedelta

from django.utils import timezone
from django.db.models import Q

from .decorators import task
from .models import Task


@task
def clean_up(task):
    """Remove stale tasks.

    Removes tasks that succeeded that are a week or older. Also
    removes any task older than a month. Runs once per day at midnight.
    """
    to_del = Task.objects.filter(
        Q(status=Task.STATUS_SUCCESS,
          updated__lt=timezone.now() - timedelta(weeks=1)) |
        Q(updated__lt=timezone.now() - timedelta(months=1))
    )
    task.log('Cleaned up: {}'.format(', '.join([o.id for o in to_del])))
    to_del.delete()
