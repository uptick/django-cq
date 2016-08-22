from threading import Thread

from django.apps import AppConfig
from channels.signals import worker_ready


def launch_checkin(*args, **kwargs):
    from .backends import worker_publish_current
    thread = Thread(name='heartbeat', target=worker_publish_current)
    thread.daemon = True
    thread.start()


class CqConfig(AppConfig):
    name = 'cq'

    def ready(self):
        worker_ready.connect(launch_checkin)
