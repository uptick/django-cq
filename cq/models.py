import time
from datetime import timedelta
import uuid
import logging

from django.db import models, transaction
from django.contrib.postgres.fields import JSONField
from django.utils import timezone
from channels import Channel

from .signature import from_signature
from .utils import rlock


logger = logging.getLogger('cq')


class Task(models.Model):
    STATUS_PENDING = 'P'
    STATUS_QUEUED = 'Q'
    STATUS_RUNNING = 'R'
    STATUS_FAILURE = 'F'
    STATUS_SUCCESS = 'S'
    STATUS_WAITING = 'W'
    STATUS_INCOMPLETE = 'I'
    STATUS_LOST = 'L'
    STATUS_CHOICES = (
        (STATUS_PENDING, 'Pending'),
        (STATUS_RUNNING, 'Running'),
        (STATUS_FAILURE, 'Failure'),
        (STATUS_SUCCESS, 'Success')
    )
    STATUS_DONE = {STATUS_FAILURE, STATUS_SUCCESS, STATUS_INCOMPLETE,
                   STATUS_LOST}

    AT_RISK_QUEUED = 'Q'
    AT_RISK_RUNNING = 'R'
    AT_RISK_CHOICES = (
        (AT_RISK_QUEUED, 'Queued'),
        (AT_RISK_RUNNING, 'Running'),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    status = models.CharField(max_length=1, choices=STATUS_CHOICES,
                              default=STATUS_PENDING)
    signature = JSONField(default={})
    details = JSONField(default={})
    parent = models.ForeignKey('self', blank=True, null=True,
                               related_name='subtasks')
    previous = models.ManyToManyField('self', related_name='next',
                                      symmetrical=False)
    waiting_on = models.ForeignKey('self', blank=True, null=True)
    submitted = models.DateTimeField(auto_now_add=True)
    started = models.DateTimeField(null=True, blank=True)
    updated = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ('-submitted',)

    def __str__(self):
        return '{} ({})'.format(self.signature, self.submitted)

    def submit(self):
        """To be run from server.
        """
        with rlock(self.id):
            if self.status != self.STATUS_PENDING:
                msg = 'Task {} cannot be submitted multiple times.'
                msg = msg.format(self.id)
                raise Exception(msg)
            self.status = self.STATUS_QUEUED

            # The database sometimes has not finished writing a commit
            # before the worker begins executing. In these cases we need
            # to wait for the commit.
            with transaction.atomic():
                self.save(update_fields=('status',))
                transaction.on_commit(lambda: Channel('cq-tasks').send({
                    'task_id': str(self.id)
                }))

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
        self.details = {}
        self.save(update_fields=('status', 'started', 'details'))
        func, args, kwargs = from_signature(self.signature)
        if result is not None:
            args = (result,) + args
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
        logger.info('Waiting task: {}'.format(self.signature['func_name']))
        self.status = self.STATUS_WAITING
        self.waiting_on = task
        if result is not None:
            logger.info('Setting task result: {} = {}'.format(
                self.signature['func_name'], result
            ))
            self.details = {
                'result': result
            }
        self.save(update_fields=('status', 'waiting_on', 'details'))

    def success(self, result=None):
        """To be run from workers.
        """
        logger.info('Task succeeded: {}'.format(self.signature['func_name']))
        self.status = self.STATUS_SUCCESS
        if result is not None:
            logger.info('Setting task result: {} = {}'.format(
                self.signature['func_name'], result
            ))
            self.details = {
                'result': result
            }
        with transaction.atomic():
            self.save(update_fields=('status', 'details'))
            transaction.on_commit(lambda: self.post_success(result))

    def post_success(self, result):
        if self.parent:
            self.parent.child_succeeded(self, result)
        for next in self.next.all():
            next.start(result)

    def child_succeeded(self, task, result):
        logger.info('Task child succeeded: {}'.format(self.signature['func_name']))
        if task == self.waiting_on and self.status not in (self.STATUS_FAILURE, self.STATUS_LOST):
            self.details = {
                'result': result
            }
            self.save(update_fields=('details',))
        if all([s.status == self.STATUS_SUCCESS for s in self.subtasks.all()]):
            logger.info('All children succeeded: {}'.format(self.signature['func_name']))
            self.success()

    def failure(self, err):
        """To be run from workers.
        """
        if self.status == self.STATUS_WAITING:
            logger.info('Task incomplete: {}'.format(self.signature['func_name']))
            self.status = self.STATUS_INCOMPLETE
        else:
            logger.info('Task failed: {}'.format(self.signature['func_name']))
            self.status = self.STATUS_FAILURE
        self.details = {
            'error': str(err)
        }
        self.save(update_fields=('status', 'details'))
        if self.parent:
            self.parent.failure(err)

    @property
    def result(self):
        return self.details.get('result', None)

    @property
    def error(self):
        return self.details.get('error', None)
