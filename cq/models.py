import time
from datetime import timedelta
import uuid

from django.db import models, transaction
from django.contrib.postgres.fields import JSONField
from django.utils import timezone
from channels import Channel

from .signature import from_signature
from .utils import rlock


class Task(models.Model):
    STATUS_PENDING = 'P'
    STATUS_QUEUED = 'Q'
    STATUS_RUNNING = 'R'
    STATUS_WAITING = 'W'
    STATUS_FAILURE = 'F'
    STATUS_SUCCESS = 'S'
    STATUS_LOST = 'L'
    STATUS_CHOICES = (
        (STATUS_PENDING, 'Pending'),
        (STATUS_RUNNING, 'Running'),
        (STATUS_FAILURE, 'Failure'),
        (STATUS_SUCCESS, 'Success')
    )
    STATUS_DONE = {STATUS_FAILURE, STATUS_SUCCESS, STATUS_LOST}

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    status = models.CharField(max_length=1, choices=STATUS_CHOICES,
                              default=STATUS_PENDING)
    signature = JSONField(default={})
    details = JSONField(default={})
    parent = models.ForeignKey('self', blank=True, null=True,
                               related_name='subtasks')
    previous = models.ManyToManyField('self', related_name='next')
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

    def waiting(self):
        self.status = self.STATUS_WAITING
        self.save(update_fields=('status',))

    def success(self, result):
        """To be run from workers.
        """
        self.status = self.STATUS_SUCCESS
        self.details = {
            'result': result
        }
        self.save(update_fields=('status', 'details'))
        if self.parent and 'result' not in self.parent.details:
            self.parent.success(result)
        for next in self.next.all():
            next.start(result)

    def failure(self, err):
        """To be run from workers.
        """
        self.status = self.STATUS_FAILURE
        self.details = {
            'error': str(err)
        }
        self.save(update_fields=('status', 'details'))
        if self.parent and 'result' not in self.parent.details:
            self.parent.failure(err)

    @property
    def result(self):
        return self.details.get('result', None)

    @property
    def error(self):
        return self.details.get('error', None)
