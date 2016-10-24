import uuid
from unittest.mock import patch
from datetime import datetime
import logging

from django.test import TestCase, override_settings
from django.utils import timezone
try:
    from django.urls import reverse
except ImportError:
    from django.core.urlresolvers import reverse
from django.contrib.auth import get_user_model
from channels.tests import (
    TransactionChannelTestCase
)
from channels.tests.base import ChannelTestCaseMixin
from croniter import croniter
from rest_framework.test import APITransactionTestCase

from ..models import Task, RepeatingTask, delay, DuplicateSubmitError
from ..decorators import task
from ..consumers import run_task
from ..backends import backend
from ..tasks import retry_tasks


User = get_user_model()


class SilentMixin(object):
    def setUp(self):
        super().setUp()
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        super().tearDown()
        logging.disable(logging.NOTSET)


@task('a')
def task_a(task, *args):
    if args:
        return args
    else:
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
    task.subtask(task_a)
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


@task
def task_h(task):
    return task.subtask(task_i).chain(task_j, (2,))


@task
def task_i(task):
    return 3


@task('j')
def task_j(task, a):
    return task.previous.result + a


def errback(task, error):
    pass


@task('k')
def task_k(task, error=False):
    task.errorback(errback)
    if error:
        raise Exception


@task(atomic=False)
def task_l(task, uuid, error=False):
    Task.objects.create(id=uuid)
    if error:
        raise Exception


@task
def task_m(task, uuid, error=False):
    Task.objects.create(id=uuid)
    if error:
        raise Exception


def check_retry(err):
    if 'hello' in str(err):
        return True
    return False


class Retry(Exception):
    pass


@task(retries=3, retry_exceptions=[Retry, check_retry])
def task_n(task, error=None):
    if error == 'exception':
        raise Retry
    elif error == 'func':
        raise Exception('hello')
    elif error == 'fail':
        raise Exception('nope')


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class DecoratorTestCase(SilentMixin, TransactionChannelTestCase):
    def test_adds_delay_function(self):
        self.assertTrue(hasattr(task_a, 'delay'))
        self.assertIsNot(task_a.delay, None)

    def test_task_is_still_a_function(self):
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


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class TaskSuccessTestCase(SilentMixin, TransactionChannelTestCase):
    def test_something(self):
        task = task_a.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class TaskFailureTestCase(SilentMixin, TransactionChannelTestCase):
    def test_something(self):
        task = task_b.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertIsNot(task.error, None)

    def test_errorbacks(self):
        task = task_k.delay(error=True)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()

    def test_retry_fail(self):
        task = task_n.delay(error='fail')
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertEqual(task.retries, 0)

    def test_retry_success(self):
        task = task_n.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(task.retries, 0)

    def test_retry_exception(self):
        task = task_n.delay(error='exception')
        run_task(self.get_next_message('cq-tasks', require=True))
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_RETRY)
        retry_tasks(retry_delay=0)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_RETRY)
        self.assertEqual(task.retries, 1)
        retry_tasks(retry_delay=0)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_RETRY)
        self.assertEqual(task.retries, 2)
        retry_tasks(retry_delay=0)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertEqual(task.retries, 3)

    def test_retry_func(self):
        task = task_n.delay(error='func')
        run_task(self.get_next_message('cq-tasks', require=True))
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_RETRY)
        retry_tasks(retry_delay=0)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_RETRY)
        self.assertEqual(task.retries, 1)
        retry_tasks(retry_delay=0)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_RETRY)
        self.assertEqual(task.retries, 2)
        retry_tasks(retry_delay=0)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.refresh_from_db()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertEqual(task.retries, 3)


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class TaskRevokeTestCase(SilentMixin, TransactionChannelTestCase):
    def test_cancel_before_launch(self):
        task = delay(task_c, (), {}, submit=False)
        task.revoke()
        task.submit()
        run_task(self.get_next_message('cq-tasks', require=False))
        run_task(self.get_next_message('cq-tasks', require=False))
        task.wait()
        self.assertEqual(task.status, task.STATUS_REVOKED)
        self.assertEqual(task.subtasks.first(), None)

    def test_cancel_after_submit(self):
        task = task_c.delay()
        task.refresh_from_db()
        task.revoke()
        run_task(self.get_next_message('cq-tasks', require=False))
        run_task(self.get_next_message('cq-tasks', require=False))
        task.wait()
        self.assertEqual(task.status, task.STATUS_REVOKED)
        self.assertEqual(task.subtasks.first(), None)

    def test_cancel_after_run(self):
        task = task_c.delay()
        run_task(self.get_next_message('cq-tasks', require=False))
        task.refresh_from_db()
        task.revoke()
        run_task(self.get_next_message('cq-tasks', require=False))
        task.wait()
        self.assertEqual(task.status, task.STATUS_REVOKED)
        self.assertEqual(task.subtasks.first().status, task.STATUS_REVOKED)


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class AsyncSubtaskTestCase(SilentMixin, TransactionChannelTestCase):
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
        task.wait(500)
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
        self.assertEqual(task.status, task.STATUS_INCOMPLETE)
        self.assertEqual(task.result, None)
        self.assertIsNot(task.error, None)

    def test_returns_subsubtask_error(self):
        task = task_g.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_INCOMPLETE)
        self.assertEqual(task.result, None)
        self.assertIsNot(task.error, None)


class SerialSubtaskTestCase(SilentMixin, TransactionChannelTestCase):
    def test_returns_own_result(self):
        result = task_d()
        self.assertEqual(result, 'd')

    def test_returns_subtask_result(self):
        result = task_c()
        self.assertEqual(result, 'a')

    def test_returns_subsubtask_result(self):
        result = task_e()
        self.assertEqual(result, 'a')


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class AsyncChainedTaskTestCase(SilentMixin, TransactionChannelTestCase):
    def test_all(self):
        task = task_h.delay()
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(task.result, 5)


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class AtomicTaskTestCase(SilentMixin, TransactionChannelTestCase):
    def test_non_atomic_success(self):
        uid = uuid.uuid4()
        task = task_l.delay(str(uid), error=False)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(Task.objects.filter(id=str(uid)).exists(), True)

    def test_non_atomic_failure(self):
        uid = uuid.uuid4()
        task = task_l.delay(str(uid), error=True)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertEqual(Task.objects.filter(id=str(uid)).exists(), True)

    def test_atomic_success(self):
        uid = uuid.uuid4()
        task = task_m.delay(str(uid), error=False)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(Task.objects.filter(id=str(uid)).exists(), True)

    def test_atomic_failure(self):
        uid = uuid.uuid4()
        task = task_m.delay(str(uid), error=True)
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_FAILURE)
        self.assertEqual(Task.objects.filter(id=str(uid)).exists(), False)


# class GetQueuedTasksTestCase(SilentMixin, TestCase):
#     def test_returns_empty(self):
#         task_ids = backend.get_queued_tasks()
#         self.assertEqual(task_ids, {})

#     @skip('Need worker disabled.')
#     def test_queued(self):
#         chan = Channel('cq-tasks')
#         chan.send({'task_id': 'one'})
#         chan.send({'task_id': 'two'})
#         task_ids = get_queued_tasks()
#         self.assertEqual(task_ids, {'one', 'two'})
#         cl = chan.channel_layer
#         while len(task_ids):
#             msg = cl.receive_many(['cq-tasks'], block=True)
#             task_ids.remove(msg[1]['task_id'])


# class GetRunningTasksTestCase(SilentMixin, TestCase):
#     def test_empty_list(self):
#         task_ids = get_running_tasks()
#         self.assertEqual(task_ids, set())

#     def test_running(self):
#         conn = Redis.from_url(settings.REDIS_URL)
#         conn.lpush('cq-current', 'one')
#         conn.lpush('cq-current', 'two')
#         task_ids = get_running_tasks()
#         self.assertEqual(task_ids, {'one', 'two'})
#         task_ids = get_running_tasks()
#         self.assertEqual(task_ids, set())


class PublishCurrentTestCase(SilentMixin, TestCase):
    def test_publish(self):
        backend.clear_current()
        backend.set_current_task('hello')
        backend.publish_current(max_its=2, sleep_time=0.1)
        backend.set_current_task('world')
        backend.publish_current(max_its=3, sleep_time=0.1)
        task_ids = backend.get_running_tasks()
        self.assertEqual(task_ids, {'hello', 'world'})
        task_ids = backend.get_running_tasks()
        self.assertEqual(task_ids, set())


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class CreateRepeatingTaskTestCase(SilentMixin, TestCase):
    def test_create(self):
        rt = RepeatingTask.objects.create(func_name='cq.tests.test_base.task_a')
        next = croniter(rt.crontab, timezone.now()).get_next(datetime)
        self.assertEqual(rt.next_run, next)


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class RunRepeatingTaskTestCase(SilentMixin, TransactionChannelTestCase):
    def test_run(self):
        rt = RepeatingTask.objects.create(func_name='cq.tests.test_base.task_a')
        task = rt.submit()
        self.assertLess(rt.last_run, timezone.now())
        self.assertGreater(rt.next_run, timezone.now())
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.result, 'a')


class ViewTestCase(SilentMixin, ChannelTestCaseMixin, APITransactionTestCase):
    def setUp(self):
        try:
            self.user = User.objects.create(
                username='a', email='a@a.org', password='a'
            )
        except:
            self.user = User.objects.create(
                email='a@a.org', password='a'
            )

    def test_create_and_get_task(self):

        # If the views aren't available, don't test.
        try:
            reverse('cqtask-list')
        except:
            return

        # Check task creation.
        data = {
            'task': 'k',
            'args': [False]
        }
        self.client.force_authenticate(self.user)
        response = self.client.post(reverse('cqtask-list'), data, format='json')
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.json().get('id', None), None)

        # Then retreival.
        id = response.json()['id']
        response = self.client.get(reverse('cqtask-detail', kwargs={'pk': id}), data, format='json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['status'], 'Q')
        run_task(self.get_next_message('cq-tasks', require=True))
        response = self.client.get(reverse('cqtask-detail', kwargs={'pk': id}), data, format='json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['status'], 'S')


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class SubmitExtraArgsTestCase(SilentMixin, TransactionChannelTestCase):
    def test_prefix_argument(self):
        task = task_a.delay_args(submit=False)
        task.submit('hello')
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(task.signature['args'][0], 'hello')
        self.assertEqual(task.result, ['hello'])

    def test_prefix_many_arguments(self):
        task = task_a.delay_args(submit=False)
        task.submit('hello', 'world')
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(task.signature['args'][0], 'hello')
        self.assertEqual(task.signature['args'][1], 'world')
        self.assertEqual(task.result, ['hello', 'world'])

    def test_prefix_arguments_with_existing(self):
        task = task_a.delay_args(args=('world',), submit=False)
        task.submit('hello')
        run_task(self.get_next_message('cq-tasks', require=True))
        task.wait()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(task.signature['args'][0], 'hello')
        self.assertEqual(task.signature['args'][1], 'world')
        self.assertEqual(task.result, ['hello', 'world'])


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class DuplicateSubmitTestCase(SilentMixin, TransactionChannelTestCase):
    def test_fails_if_pending(self):
        task = task_a.delay(submit=False)
        with self.assertRaises(DuplicateSubmitError):
            task.submit()

    def test_skips_if_revoked(self):
        task = task_a.delay_args(submit=False)
        task.revoke()
        task.submit()
        with self.assertRaises(AssertionError):
            run_task(self.get_next_message('cq-tasks', require=True))
        self.assertEqual(task.status, task.STATUS_REVOKED)
