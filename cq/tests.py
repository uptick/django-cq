from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone
from channels.tests import TransactionChannelTestCase, ChannelTestCase
from channels import Channel
from channels.asgi import channel_layers

from .models import Task
from .decorators import task
from .consumers import run_task
from .backends import get_queued_tasks


@task
def task_a(task):
    return 'a'


@task
def task_b(task):
    raise Exception('b')
    return 'b'


@task
def task_c(task):
    sub = task.subtask(task_a)
    return sub


@task
def task_d(task):
    sub = task.subtask(task_a)
    return 'd'


@task
def task_e(task):
    sub = task.subtask(task_c)
    return sub


@task
def task_f(task):
    sub = task.subtask(task_b)
    return sub


@task
def task_g(task):
    sub = task.subtask(task_f)
    return sub


class DecoratorTestCase(TransactionChannelTestCase):
    def test_adds_delay_function(self):
        self.assertTrue(hasattr(task_a, 'delay'))
        self.assertIsNot(task_a.delay, None)

    def test_task_is_still_task(self):
        self.assertEqual(task_a(), 'a')
        self.assertEqual(Task.objects.all().count(), 0)

    @patch('cq.models.Task.submit')
    def test_delay_creates_task(self, submit):
        before = timezone.now()
        task = task_a.delay()
        after = timezone.now()
        self.assertIsNot(task, None)
        self.assertGreater(len(str(task.id)), 10)
        self.assertGreater(task.submitted, before)
        self.assertLess(task.submitted, after)


class TaskQueuedTestCase(TestCase):
    pass


class TaskSuccessTestCase(TransactionChannelTestCase):
    def test_something(self):
        task = task_a.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)


class TaskFailureTestCase(TransactionChannelTestCase):
    def test_something(self):
        task = task_b.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertIsNot(task.result.get('error', None), None)


class DBLatencyTestCase(TestCase):
    pass


class AsyncSubtaskTestCase(TransactionChannelTestCase):
    def test_returns_own_result(self):
        task = task_d.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(task.result, 'd')

    def test_returns_subtask_result(self):
        task = task_c.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(task.result, 'a')

    def test_returns_subsubtask_result(self):
        task = task_e.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(task.result, 'a')

    def test_parent_tasks_enter_waiting_state(self):
        task = task_e.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_WAITING)
        subtask = task.subtasks.first()
        self.assertEqual(subtask.status, Task.STATUS_WAITING)
        subsubtask = subtask.subtasks.first()
        self.assertEqual(subsubtask.status, Task.STATUS_QUEUED)

    def test_returns_subtask_error(self):
        task = task_f.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertEqual(task.result, None)
        self.assertIsNot(task.error, None)

    def test_returns_subsubtask_error(self):
        task = task_g.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertEqual(task.result, None)
        self.assertIsNot(task.error, None)


class SerialSubtaskTestCase(TransactionChannelTestCase):
    def test_returns_own_result(self):
        result = task_d()
        self.assertEqual(result, 'd')

    def test_returns_subtask_result(self):
        result = task_c()
        self.assertEqual(result, 'a')

    def test_returns_subsubtask_result(self):
        result = task_e()
        self.assertEqual(result, 'a')


class GetQueuedTasksTestCase(TestCase):
    def test_returns_empty(self):
        task_ids = get_queued_tasks()
        self.assertEqual(task_ids, {})

    def test_queued(self):
        chan = Channel('cq-tasks')
        chan.send({'task_id': 'one'})
        chan.send({'task_id': 'two'})
        task_ids = get_queued_tasks()
        self.assertEqual(task_ids, {'one', 'two'})
        cl = chan.channel_layer
        while len(task_ids):
            msg = cl.receive_many(['cq-tasks'], block=True)
            task_ids.remove(msg[1]['task_id'])
