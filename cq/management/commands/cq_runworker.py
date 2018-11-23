import logging
import time
from threading import Thread

from channels.management.commands.runworker import Command as BaseCommand
from django_redis import get_redis_connection

from django.conf import settings

from ...consumers import run_task
from ...utils import get_redis_key

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
        if getattr(settings, 'CQ_BACKEND', '').lower() == 'redis':
            self.handle_redis_backend()
        else:
            super().handle(*args, **options)

    def handle_redis_backend(self):
        while True:
            conn = get_redis_connection()
            while True:
                message = conn.brpop(get_redis_key('cq'))
                try:
                    run_task(message[1].decode())
                except Exception as e:
                    logger.error(str(e))
                time.sleep(1)
