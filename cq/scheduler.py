import logging
import time
from datetime import timedelta
from traceback import format_exc

from django.core.cache import cache
from django.db.utils import ProgrammingError
from django.utils import timezone

from .models import RepeatingTask
from .utils import redis_connection, get_redis_key

logger = logging.getLogger('cq')


def perform_scheduling():
    logger.debug('cq-scheduler: performing scheduling started')
    with cache.lock('cq:scheduler:lock', timeout=10):
        logger.debug('cq-scheduler: checking for scheduled tasks')
        now = timezone.now()
        try:
            rtasks = RepeatingTask.objects.filter(next_run__lte=now)
            logger.info('cq-scheduler: have {} repeating task(s) ready'.format(rtasks.count()))
            for rt in rtasks:
                try:
                    rt.submit()
                except Exception as e:
                    # Don't terminate if a submit fails.
                    logger.error(format_exc())
        except ProgrammingError:
            logger.warning('CQ scheduler not running, DB is out of date.')
    logger.debug('cq-scheduler: performing scheduling finished')


def scheduler_internal():
    logger.debug('cq-scheduler: determining winning scheduler')
    am_scheduler = False
    with redis_connection() as conn:
        if conn.set(get_redis_key('cq:scheduler'), 'dummy', nx=True, ex=30):
            # conn.expire('cq:scheduler', 30)
            am_scheduler = True
    if am_scheduler:
        logger.debug('cq-scheduler: winner')
        perform_scheduling()
    else:
        logger.debug('cq-scheduler: loser')
    now = timezone.now()
    delay = ((now + timedelta(minutes=1)).replace(second=0, microsecond=0) - now).total_seconds()
    logger.debug('cq-scheduler: waiting {} seconds for next schedule attempt'.format(delay))
    time.sleep(delay)


def scheduler(*args, **kwargs):
    logger.info('cq-scheduler: Scheduler thread active.')
    while 1:
        try:
            scheduler_internal()
        except Exception as e:
            logger.error(format_exc())
            time.sleep(0.5)
