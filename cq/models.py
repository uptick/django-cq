import re
import time
import json
from datetime import datetime
from datetime import timedelta
import uuid
from traceback import format_tb
import logging

from django.db import models, transaction
from django.contrib.postgres.fields import JSONField
from django.conf import settings
from django.utils import timezone
from django.core.exceptions import ValidationError
from django.core.cache import cache
from channels import Channel, DEFAULT_CHANNEL_LAYER
from asgi_redis import RedisChannelLayer
from croniter import croniter

from .task import from_signature, to_signature, to_func_name, TaskFunc
from .managers import TaskManager
from .utils import import_attribute, redis_connection


logger = logging.getLogger('cq')


class CQError(Exception):
    pass


class DuplicateSubmitError(CQError):
    pass


class Task(models.Model):
    """A persistent representation of a background task.
    """
    STATUS_PENDING = 'P'
    STATUS_RETRY = 'Y'
    STATUS_QUEUED = 'Q'
    STATUS_RUNNING = 'R'
    STATUS_FAILURE = 'F'
    STATUS_SUCCESS = 'S'
    STATUS_WAITING = 'W'
    STATUS_INCOMPLETE = 'I'
    STATUS_LOST = 'L'
    STATUS_REVOKED = 'E'
    STATUS_CHOICES = (
        (STATUS_PENDING, 'Pending'),
        (STATUS_RETRY, 'Retry'),
        (STATUS_QUEUED, 'Queued'),
        (STATUS_RUNNING, 'Running'),
        (STATUS_FAILURE, 'Failure'),
        (STATUS_SUCCESS, 'Success'),
        (STATUS_WAITING, 'Waiting'),
        (STATUS_INCOMPLETE, 'Incomplete'),
        (STATUS_LOST, 'Lost'),
        (STATUS_REVOKED, 'Revoked')
    )
    STATUS_DONE = {STATUS_FAILURE, STATUS_SUCCESS, STATUS_INCOMPLETE,
                   STATUS_LOST, STATUS_REVOKED}
    STATUS_ERROR = {STATUS_FAILURE, STATUS_LOST, STATUS_INCOMPLETE,
                    STATUS_REVOKED}
    STATUS_ACTIVE = {STATUS_PENDING, STATUS_QUEUED, STATUS_RUNNING,
                     STATUS_WAITING}

    AT_RISK_NONE = 'N'
    AT_RISK_QUEUED = 'Q'
    AT_RISK_RUNNING = 'R'
    AT_RISK_CHOICES = (
        (AT_RISK_NONE, 'None'),
        (AT_RISK_QUEUED, 'Queued'),
        (AT_RISK_RUNNING, 'Running'),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    status = models.CharField(max_length=1, choices=STATUS_CHOICES,
                              default=STATUS_PENDING, db_index=True)
    signature = JSONField(default={}, blank=True)
    details = JSONField(default={}, blank=True)
    parent = models.ForeignKey('self', blank=True, null=True,
                               related_name='subtasks')
    previous = models.ForeignKey('self', related_name='next', blank=True,
                                 null=True, on_delete=models.SET_NULL)
    waiting_on = models.ForeignKey('self', blank=True, null=True,
                                   on_delete=models.SET_NULL)
    submitted = models.DateTimeField(auto_now_add=True)
    started = models.DateTimeField(null=True, blank=True)
    finished = models.DateTimeField(null=True, blank=True)
    result_ttl = models.PositiveIntegerField(default=1800, blank=True)
    result_expiry = models.DateTimeField(null=True, blank=True)
    at_risk = models.CharField(max_length=1, choices=AT_RISK_CHOICES,
                               default=AT_RISK_NONE)
    retries = models.PositiveIntegerField(default=0)
    last_retry = models.DateTimeField(null=True, blank=True)
    force_chain = models.BooleanField(default=False)

    objects = TaskManager()

    class Meta:
        ordering = ('-submitted',)

    def __str__(self):
        return '{} - {}'.format(self.id, self.func_name)

    def retry(self):
        self.status = self.STATUS_PENDING
        self.started = None
        self.finished = None
        self.details = {}
        self.at_risk = self.AT_RISK_NONE
        self.retries += 1
        self.last_retry = timezone.now()
        self.save(update_fields=('status', 'started', 'finished',
                                 'details', 'at_risk', 'retries',
                                 'last_retry'))
        self.submit()

    def submit(self, *pre_args):
        """To be run from server.
        """
        with cache.lock(str(self.id), timeout=2):

            # Need to reload just in case we've been modified elsewhere.
            self.refresh_from_db()

            # If we've been moved to revoke, don't run. If we're anything
            # other than pending, error.
            if self.status == self.STATUS_REVOKED:
                return
            elif self.status != self.STATUS_PENDING:
                msg = 'Task {} cannot be submitted multiple times.'
                msg = msg.format(self.id)
                raise DuplicateSubmitError(msg)
            self.status = self.STATUS_QUEUED

            # Prepend arguments.
            if len(pre_args) > 0:
                func, args, kwargs = from_signature(self.signature)
                args = pre_args + tuple(args)
                self.signature = to_signature(func, args, kwargs)

            # The database sometimes has not finished writing a commit
            # before the worker begins executing. In these cases we need
            # to wait for the commit.
            with transaction.atomic():
                self.save(update_fields=('status', 'signature'))
                transaction.on_commit(lambda: self.send())

    def send(self):
        layer = getattr(settings, 'CQ_CHANNEL_LAYER', DEFAULT_CHANNEL_LAYER)
        logger.debug('Sending CQ message on "{}" layer.'.format(layer))
        try:
            Channel('cq-tasks', alias=layer).send({
                'task_id': str(self.id),
            }, immediately=True)
        except RedisChannelLayer.ChannelFull:
            with cache.lock(str(self.id), timeout=2):
                self.status = self.STATUS_RETRY
                self.save(update_fields=('status',))

    def wait(self, timeout=None):
        """Wait for task to finish. To be called from server.
        """
        start = timezone.now()
        end = start
        if timeout is not None:
            start += timedelta(milliseconds=timeout)
        delta = timedelta(milliseconds=500)
        self.refresh_from_db()
        while (self.status not in self.STATUS_DONE and
               (timeout is None or start < end)):
            time.sleep(0.5)
            self.refresh_from_db()
            start += delta

    def pre_start(self):
        self.status = self.STATUS_RUNNING
        self.started = timezone.now()
        self.save(update_fields=('status', 'started'))

        # Ensure our logs are fresh.
        self._task_logs = []
        cache.delete(self._get_log_key())

    def start(self, result=None, pre_start=True):
        """To be run from workers.
        """
        if pre_start:
            self.pre_start()
        func, args, kwargs = from_signature(self.signature)
        if result is not None:
            args = (result,) + tuple(args)
        task_func = TaskFunc.get_task(self.signature['func_name'])
        if task_func.atomic:
            with transaction.atomic():
                return func(*args, task=self, **kwargs)
        else:
            return func(*args, task=self, **kwargs)

    def revoke(self):
        with cache.lock(str(self.id), timeout=2):
            if self.status not in self.STATUS_DONE:
                self.status = self.STATUS_REVOKED
                self.save(update_fields=('status',))
            for child in self.subtasks.all():
                child.revoke()
            for next in self.next.all():
                next.revoke()

    def subtask(self, func, args=(), kwargs={}, **kw):
        """Launch a subtask.

        Subtasks are run at the same time as the current task. The current
        task will not be considered complete until the subtask finishes.
        """
        return delay(func, args, kwargs, parent=self, **kw)

    def chain(self, func, args=(), kwargs={}, **kw):
        """Chain a task.

        Chained tasks are run after completion of the current task, and are
        passed the result of the current task.
        """
        return chain(func, args, kwargs, previous=self, **kw)

    def errorback(self, func, args=(), kwargs={}):
        self.details.setdefault('errbacks', []).append(
            to_signature(func, args, kwargs)
        )
        self.save(update_fields=('details',))

    def waiting(self, task=None, result=None):
        logger.info('Waiting task: {}'.format(self.func_name))
        self.status = self.STATUS_WAITING
        self.waiting_on = task
        if task is not None and task.parent != self:
            assert task.parent is None
            task.parent = self
            task.save(update_fields=('parent',))
        if result is not None:
            logger.info('Setting task result: {} = {}'.format(
                self.func_name, result
            ))
            self.details['result'] = result

        # Must publish all remaining local logs to REDIS.
        self._publish_logs()

        with transaction.atomic():
            self.save(update_fields=('status', 'waiting_on', 'details'))
            transaction.on_commit(lambda: self.post_waiting())

    def success(self, result=None):
        """To be run from workers.
        """
        logger.info('Task succeeded: {}'.format(self.func_name))
        self.status = self.STATUS_SUCCESS
        if result is not None:
            logger.info('Setting task result: {} = {}'.format(
                self.func_name, result
            ))
            self.details['result'] = result
        self.finished = timezone.now()
        self.result_expiry = self.finished + timedelta(seconds=self.result_ttl)
        self._store_logs()
        with transaction.atomic():
            self.save(update_fields=('status', 'details', 'finished', 'result_expiry'))
            transaction.on_commit(lambda: self.post_success(self.result))

    def post_success(self, result):
        if self.parent:
            self.parent.child_succeeded(self, result)
        self.launch_next()

    def post_waiting(self):
        self.launch_subtasks()

    def launch_subtasks(self):

        # Launch subtasks, but don't fire off any chained subtasks. Chained
        # tasks get registered as subtasks also in order to pass logs.
        for next in self.subtasks.all():
            if next.previous is None:
                next.submit()

    def launch_next(self, force_chain=False):
        for next in self.next.all():
            if not force_chain or next.force_chain:
                next.submit()

    def _store_logs(self):
        if self.parent:
            return
        key = self._get_log_key()
        with redis_connection() as con:
            try:
                logs = con.lrange(key, 0, -1)
                logs = [json.loads(l.decode()) for l in logs]
            except:
                logs = []
        try:
            logs.extend(self._task_logs)
        except AttributeError:
            pass
        if 'logs' not in self.details:
            self.details['logs'] = []
        self.details['logs'].extend(logs)
        cache.delete(key)

    def child_succeeded(self, task, result):
        logger.info('Task child succeeded: {}'.format(self.func_name))
        if task == self.waiting_on and self.status not in self.STATUS_ERROR:
            logger.info('Setting task result: {} = {}'.format(
                self.func_name, result
            ))
            self.details['result'] = result
            self.save(update_fields=('details',))
        if all([s.status == self.STATUS_SUCCESS for s in self.subtasks.all()]):
            logger.info('All children succeeded: {}'.format(self.func_name))
            self.success()

    def failure(self, err, retry=False):
        """To be run from workers.
        """

        # Set the error details.
        self.details['error'] = str(err)
        self.details['exception'] = err.__class__.__name__
        try:
            self.details['traceback'] = ''.join(format_tb(err.__traceback__))
        except:
            pass

        # Set the status and start formatting the output message.
        if self.status == self.STATUS_WAITING or self.status == self.STATUS_INCOMPLETE:
            msg = 'Task incomplete: {}'.format(self.func_name)
            self.status = self.STATUS_INCOMPLETE
        else:
            msg = 'Task failed: {}'.format(self.func_name)
            self.status = self.STATUS_FAILURE

        # Finish the message.
        msg += '\nError: {}'.format(self.details['error'])
        if 'traceback' in self.details:
            msg += '\nTraceback:\n{}'.format(self.details['traceback'])
        logger.error(msg)

        if retry:
            self.status = self.STATUS_RETRY
        self.finished = timezone.now()
        self._store_logs()
        self.save(update_fields=('status', 'details', 'finished'))

        if not retry:
            if self.parent:
                self.parent.failure(err)
            for eb in self.details.get('errbacks', []):
                func, args, kwargs = from_signature(eb)
                func(*((self, err,) + tuple(args)), **kwargs)

            # Check if we want to force the subsequent chained
            # items to run.
            self.launch_next(force_chain=True)

    def log(self, msg, level=logging.INFO, origin=None, publish=True,
            limit=40):
        """Log to the task, and to the system logger.

        Will push the logged message to the topmost task.
        """
        if self.parent:
            self.parent.log(msg, level, origin or self, publish=publish)
        else:
            logger.log(level, msg)
            data = {
                'message': msg,
                'timestamp': str(timezone.now())
            }
            if origin:
                data['origin'] = str(origin.id)
            try:
                self._task_logs.append(data)
            except AttributeError:
                self._task_logs = [data]

            # Don't try to set too much in the cache, it can cause
            # problems. Instead, cap it at the past `limit` logs. Also, use
            # `publish` to control when publishing happens.
            if publish:
                self._publish_logs(limit)

    def _publish_logs(self, limit=40):
        key = self._get_log_key()
        logs = [json.dumps(l) for l in self._task_logs[-limit:]]
        if logs:
            with redis_connection() as con:
                con.rpush(key, *logs)
                con.ltrim(key, -limit, -1)
        self._task_logs = []

    @property
    def result(self):
        return self.details.get('result', None)

    @property
    def error(self):
        return self.details.get('error', None)

    @property
    def logs(self):
        logs = self.details.get('logs', None)
        if logs is None:
            with redis_connection() as con:
                key = self._get_log_key()
                try:
                    logs = con.lrange(key, 0, -1)
                except:
                    logs = []
            logs = [json.loads(l.decode()) for l in logs]
        return logs

    @property
    def func_name(self):
        return self.signature.get('func_name', None)

    def format_logs(self):
        return '\n'.join([l['message'] for l in self.logs])

    def _get_log_key(self):
        return 'cq:{}:logs'.format(self.id)


def validate_cron(value):
    if value.strip() != value:
        raise ValidationError('Leading nor trailing spaces are allowed')
    columns = value.split()
    if columns != value.split(' '):
        raise ValidationError('Use only a single space as a column separator')
    if len(columns) != 5:
        raise ValidationError('Entry has to consist of exactly 5 columns')
    pattern = r'^(\*|\d+(-\d+)?(,\d+(-\d+)?)*)(/\d+)?$'
    p = re.compile(pattern)
    for i, c in enumerate(columns):
        if not p.match(c):
            raise ValidationError("Incorrect value {} in column {}".format(
                c, i + 1
            ))


def validate_func_name(value):
    """Try to import a function before accepting it.
    """
    try:
        import_attribute(value)
    except:
        raise ValidationError('Unable to import task.')


class RepeatingTask(models.Model):
    """Basic repeating tasks.

    Uses CRON style strings to set repeating tasks.
    """
    crontab = models.CharField(max_length=100, default='* * * * *',
                               validators=[validate_cron],
                               help_text='Minute Hour Day Month Weekday')
    func_name = models.CharField(max_length=256, validators=[validate_func_name])
    args = JSONField(default=[], blank=True)
    kwargs = JSONField(default={}, blank=True)
    result_ttl = models.PositiveIntegerField(default=1800, blank=True)
    last_run = models.DateTimeField(blank=True, null=True)
    next_run = models.DateTimeField(blank=True, null=True, db_index=True)
    coalesce = models.BooleanField(default=True)

    def __str__(self):
        if self.last_run:
            return '{} ({})'.format(self.func_name, self.last_run)
        else:
            return self.func_name

    def submit(self):
        if self.coalesce and Task.objects.active(signature__func_name=self.func_name):
            logger.info('Coalescing task: {}'.format(self.func_name))
            return None
        logger.info('Launching scheduled task: {}'.format(self.func_name))
        with transaction.atomic():
            task = delay(self.func_name, tuple(self.args), self.kwargs,
                         submit=False, result_ttl=self.result_ttl)
            self.last_run = timezone.now()
            self.update_next_run()
            self.save(update_fields=('last_run', 'next_run'))
        task.submit()
        return task

    def update_next_run(self):
        self.next_run = croniter(self.crontab, timezone.localtime(timezone.now())).get_next(datetime)

    @classmethod
    def schedule(cls, crontab, func, args=(), kwargs={}):
        return schedule_task(cls, crontab, func, args, kwargs)


def schedule_task(cls, crontab, func, args=(), kwargs={}, **_kwargs):
    """Create a repeating task.
    """
    # This is mostly for creating scheduled tasks in migrations. The
    # signals don't run in migrations, so we need to explicitly set
    # the `next_run` value.
    next = croniter(crontab, timezone.localtime(timezone.now())).get_next(datetime)
    return cls.objects.create(
        crontab=crontab,
        func_name=to_func_name(func),
        args=args,
        kwargs=kwargs,
        next_run=next,
        **_kwargs
    )


def chain(func, args, kwargs, parent=None, previous=None, submit=True,
          **kw):
    """Run a task after an existing task.

    The result is passed as the first argument to the chained task.
    If no parent is specified, automatically use the parent of the
    predecessor. Note that I'm not sure this is the correct behavior,
    but is useful for making sure logs to where they should.
    """
    sig = to_signature(func, args, kwargs)
    if parent is None and previous:
        parent = previous.parent
    task = Task.objects.create(signature=sig, parent=parent, previous=previous,
                               **kw)

    # Need to check immediately if the parent task has completed and
    # launch the subtask if so.
    if parent is not None and submit:
        with cache.lock(str(parent.id), timeout=2):
            parent.refresh_from_db()
            if parent.status == Task.STATUS_SUCCESS:
                task.submit()

    # If we have no parent, and we want to submit then do so now. This
    # happens for a straight-up delay.
    elif parent is None and submit:
        task.submit()

    return task


def delay(func, args, kwargs, parent=None, submit=True, **kw):
    task = chain(func, args, kwargs, parent, submit=submit, **kw)
    # if submit:
    #     task.submit()
    return task
