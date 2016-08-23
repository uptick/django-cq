import re
import time
from datetime import datetime
from datetime import timedelta
import uuid
import logging

from django.db import models, transaction
from django.contrib.postgres.fields import JSONField
from django.utils import timezone
from django.core.exceptions import ValidationError
from channels import Channel
from asgi_redis import RedisChannelLayer
from croniter import croniter

from .signature import from_signature, to_signature, to_func_name
from .utils import rlock, import_attribute


logger = logging.getLogger('cq')


class Task(models.Model):
    STATUS_PENDING = 'P'
    STATUS_RETRY = 'Y'
    STATUS_QUEUED = 'Q'
    STATUS_RUNNING = 'R'
    STATUS_FAILURE = 'F'
    STATUS_SUCCESS = 'S'
    STATUS_WAITING = 'W'
    STATUS_INCOMPLETE = 'I'
    STATUS_LOST = 'L'
    STATUS_CHOICES = (
        (STATUS_PENDING, 'Pending'),
        (STATUS_RETRY, 'Retry'),
        (STATUS_QUEUED, 'Queued'),
        (STATUS_RUNNING, 'Running'),
        (STATUS_FAILURE, 'Failure'),
        (STATUS_SUCCESS, 'Success'),
        (STATUS_WAITING, 'Waiting'),
        (STATUS_INCOMPLETE, 'Incomplete'),
        (STATUS_LOST, 'Lost')
    )
    STATUS_DONE = {STATUS_FAILURE, STATUS_SUCCESS, STATUS_INCOMPLETE,
                   STATUS_LOST}
    STATUS_ERROR = {STATUS_FAILURE, STATUS_LOST, STATUS_INCOMPLETE}

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
                                 null=True)
    waiting_on = models.ForeignKey('self', blank=True, null=True)
    submitted = models.DateTimeField(auto_now_add=True)
    started = models.DateTimeField(null=True, blank=True)
    finished = models.DateTimeField(null=True, blank=True)
    result_ttl = models.PositiveIntegerField(default=1800, blank=True)
    result_expiry = models.DateTimeField(null=True, blank=True)
    at_risk = models.CharField(max_length=1, choices=AT_RISK_CHOICES,
                               default=AT_RISK_NONE)

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
        self.submit()

    def submit(self, *pre_args):
        """To be run from server.
        """
        with rlock(self.id):
            if self.status != self.STATUS_PENDING:
                msg = 'Task {} cannot be submitted multiple times.'
                msg = msg.format(self.id)
                raise Exception(msg)
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
        try:
            Channel('cq-tasks').send({
                'task_id': str(self.id)
            })
        except RedisChannelLayer.ChannelFull:
            self.status = self.STATUS_RETRY
            self.save(update_fields=('status',))

    def wait(self, timeout=2000):
        """Wait for task to finish. To be called from server.
        """
        start = timezone.now()
        end = start + timedelta(milliseconds=timeout)
        delta = timedelta(milliseconds=500)
        self.refresh_from_db()
        while self.status not in self.STATUS_DONE and start < end:
            time.sleep(0.5)
            self.refresh_from_db()
            start += delta

    def start(self, result=None):
        """To be run from workers.
        """
        self.status = self.STATUS_RUNNING
        self.started = timezone.now()
        self.save(update_fields=('status', 'started'))
        func, args, kwargs = from_signature(self.signature)
        if result is not None:
            args = (result,) + tuple(args)
        with transaction.atomic():
            return func(*args, task=self, **kwargs)

    def subtask(self, func, args=(), kwargs={}):
        """Launch a subtask.

        Subtasks are run at the same time as the current task. The current
        task will not be considered complete until the subtask finishes.
        """
        from .task import delay
        return delay(func, args, kwargs, parent=self)

    def chain(self, func, args=(), kwargs={}):
        """Chain a task.

        Chained tasks are run after completion of the current task, and are
        passed the result of the current task.
        """
        from .task import chain
        return chain(func, args, kwargs, previous=self)

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
        self.save(update_fields=('status', 'waiting_on', 'details'))

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
        with transaction.atomic():
            self.save(update_fields=('status', 'details', 'finished', 'result_expiry'))
            transaction.on_commit(lambda: self.post_success(self.result))

    def post_success(self, result):
        if self.parent:
            self.parent.child_succeeded(self, result)
        for next in self.next.all():
            next.submit(result)

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

    def failure(self, err):
        """To be run from workers.
        """
        if self.status == self.STATUS_WAITING:
            logger.info('Task incomplete: {}'.format(self.func_name))
            self.status = self.STATUS_INCOMPLETE
        else:
            logger.info('Task failed: {}'.format(self.func_name))
            self.status = self.STATUS_FAILURE
        self.details['error'] = str(err)
        self.finished = timezone.now()
        self.save(update_fields=('status', 'details', 'finished'))
        if self.parent:
            self.parent.failure(err)

    def log(self, msg, origin=None):
        """Log to the task, and to the system logger.

        Will push the logged message to the topmost task.
        """
        if self.parent:
            self.parent.log(msg, origin or self)
        else:
            logger.info(msg)
            data = {
                'message': msg,
                'timestamp': str(timezone.now())
            }
            if origin:
                data['origin'] = origin.id
            self.details.setdefault('logs', []).append(data)

    @property
    def result(self):
        return self.details.get('result', None)

    @property
    def error(self):
        return self.details.get('error', None)

    @property
    def logs(self):
        return self.details.get('logs', [])

    @property
    def func_name(self):
        return self.signature.get('func_name', None)

    def format_logs(self):
        return '\n'.join([l.message for l in self.logs])


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
    try:
        import_attribute(value)
    except:
        raise ValidationError('Unable to import task.')


def schedule_task(cls, crontab, func, args=(), kwargs={}, **_kwargs):
    return cls.objects.create(
        crontab=crontab,
        func_name=to_func_name(func),
        args=args,
        kwargs=kwargs,
        next_run=croniter(crontab, timezone.now()).get_next(datetime),
        **_kwargs
    )


class RepeatingTask(models.Model):
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
        from .task import delay
        logger.info('Launching scheduled task: {}'.format(self.func_name))
        task = delay(self.func_name, tuple(self.args), self.kwargs,
                     result_ttl=self.result_ttl)
        self.last_run = timezone.now()
        self.update_next_run()
        self.save(update_fields=('last_run', 'next_run'))
        return task

    def update_next_run(self):
        self.next_run = croniter(self.crontab, timezone.now()).get_next(datetime)

    @classmethod
    def schedule(cls, crontab, func, args=(), kwargs={}):
        return schedule_task(cls, crontab, func, args, kwargs)
