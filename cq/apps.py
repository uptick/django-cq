import logging

from django.apps import AppConfig
from django.conf import settings
from django.core.cache import cache
from django.db.utils import ProgrammingError
from django.utils.module_loading import import_module

logger = logging.getLogger('cq')


def scan_tasks(*args, **kwargs):
    for app_name in settings.INSTALLED_APPS:
        try:
            import_module('.'.join([app_name, 'tasks']))
        except ImportError:
            pass


def requeue_tasks(*args, **kwargs):
    from cq.models import Task
    lock = 'RETRY_QUEUED_TASKS'
    with cache.lock(lock, timeout=10):
        # Find all Queued tasks and set them to Retry, since they get stuck after a reboot
        try:
            tasks = Task.objects.filter(status=Task.STATUS_QUEUED).update(status=Task.STATUS_RETRY)
            logger.info('Requeued {} tasks'.format(tasks))
        except ProgrammingError:
            logger.warning("Failed requeuing tasks; database likely hasn't had migrations run.")
            pass


class CqConfig(AppConfig):
    name = 'cq'

    def ready(self):
        import cq.signals
        scan_tasks()
        requeue_tasks()
