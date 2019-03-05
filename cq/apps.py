import logging

from django.apps import AppConfig
from django.conf import settings
from django.utils.module_loading import import_module

logger = logging.getLogger('cq')


def scan_tasks(*args, **kwargs):
    for app_name in settings.INSTALLED_APPS:
        try:
            import_module('.'.join([app_name, 'tasks']))
        except ImportError:
            pass


class CqConfig(AppConfig):
    name = 'cq'

    def ready(self):
        import cq.signals
        scan_tasks()
