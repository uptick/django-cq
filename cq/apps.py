from threading import Thread

from django.apps import AppConfig
from channels import Channel
from channels.signals import worker_ready


def launch_checkin():
    thread = Thread(name='heartbeat', target=heartbeat)
    thread.daemon = True
    thread.start()


class CqConfig(AppConfig):
    name = 'cq'

    def ready(self):
        worker_ready.connect(launch_heartbeat)
