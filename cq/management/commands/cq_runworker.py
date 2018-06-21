import logging
from threading import Thread

from channels.management.commands.runworker import Command as BaseCommand

from django.conf import settings

logger = logging.getLogger('cq')


def launch_scheduler(*args, **kwargs):
    from ...scheduler import scheduler
    logger.info('Launching CQ scheduler.')
    thread = Thread(name='scheduler', target=scheduler)
    thread.daemon = True
    thread.start()


class Command(BaseCommand):
    def handle(self, *args, **options):
        if getattr(settings, 'CQ_SCHEDULER', True):
            launch_scheduler()
        super().handle(*args, **options)
