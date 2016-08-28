from threading import Thread
import logging

from django.apps import AppConfig
from django.conf import settings
from django.utils.module_loading import import_module
from channels.signals import worker_process_ready


logger = logging.getLogger('cq')


def launch_scheduler(*args, **kwargs):
    from .scheduler import scheduler
    logger.info('Launching CQ scheduler.')
    thread = Thread(name='scheduler', target=scheduler)
    thread.daemon = True
    thread.start()


def launch_checkin(*args, **kwargs):
    from .backends import backend
    thread = Thread(name='checkin', target=backend.publish_current)
    thread.daemon = True
    thread.start()


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
        if getattr(settings, 'CQ_SCHEDULER', True):
            worker_process_ready.connect(launch_scheduler)
        worker_process_ready.connect(scan_tasks)
        worker_process_ready.connect(launch_checkin)
