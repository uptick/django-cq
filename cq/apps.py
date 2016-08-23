from threading import Thread
import logging

from django.apps import AppConfig
from django.conf import settings
from channels.signals import worker_ready


logger = logging.getLogger('cq')


def launch_scheduler():
    from .backends import scheduler
    logger.info('Launching CQ scheduler.')
    thread = Thread(name='scheduler', target=scheduler)
    thread.daemon = True
    thread.start()


def launch_checkin(*args, **kwargs):
    from .backends import worker_publish_current
    thread = Thread(name='checkin', target=worker_publish_current)
    thread.daemon = True
    thread.start()


class CqConfig(AppConfig):
    name = 'cq'

    def ready(self):
        import cq.signals
        if getattr(settings, 'CQ_SCHEDULER', True):
            launch_scheduler()
        worker_ready.connect(launch_checkin)
