import logging

from django.apps import AppConfig
from django.conf import settings
from django.core.cache import cache
from django.db.backends.signals import connection_created
from django.db.models.signals import post_migrate
from django.utils.module_loading import import_module

logger = logging.getLogger('cq')


migration_happened = False


def post_migration_callback(sender, **kwargs):
    global migration_happened
    migration_happened = True


def scan_tasks(*args, **kwargs):
    for app_name in settings.INSTALLED_APPS:
        try:
            import_module('.'.join([app_name, 'tasks']))
        except ImportError:
            pass


def requeue_tasks(sender, connection):
    from cq.models import Task
    lock = 'RETRY_QUEUED_TASKS'
    with cache.lock(lock, timeout=2):
        # Find all Queued tasks and set them to Retry, since they get stuck after a reboot
        Task.objects.filter(status=Task.STATUS_QUEUED).update(status=Task.STATUS_RETRY)


class CqConfig(AppConfig):
    name = 'cq'

    def ready(self):
        import cq.signals
        scan_tasks()
        # Run requeue_tasks() only after migrations are run
        post_migrate.connect(post_migration_callback, sender=self)
        if not migration_happened:
            connection_created.connect(requeue_tasks, sender=self)
