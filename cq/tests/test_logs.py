from django.test import override_settings
from channels.tests import (
    TransactionChannelTestCase
)

from ..decorators import task
from ..consumers import run_task
from ..models import Task
from .test_base import SilentMixin


@task
def task_a(cqt, publish=True):
    cqt.log('First log from task_a.', publish=publish)
    cqt.subtask(task_b, {'publish': publish})
    cqt.log('Second log from task_a.', publish=publish)
    return 'a'


@task
def task_b(cqt, publish=True):
    cqt.log('Log from task_b.', publish=publish)
    return 'b'


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class BasicLogTestCase(SilentMixin, TransactionChannelTestCase):
    def test_logs_stored_on_model(self):
        task = task_b.delay()
        while run_task(self.get_next_message('cq-tasks')):
            pass
        task.wait()
        logs = task.format_logs()
        self.assertEqual(logs, (
            'Log from task_b.'
        ))

    def test_logs_from_subtasks(self):
        task = task_a.delay()
        while 1:
            msg = self.get_next_message('cq-tasks')
            if not msg:
                break
            run_task(msg)
        task.wait()
        logs = task.format_logs()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(logs, (
            'First log from task_a.\n'
            'Second log from task_a.\n'
            'Log from task_b.'
        ))

    def test_no_publish(self):
        task = task_a.delay(publish=False)
        while 1:
            msg = self.get_next_message('cq-tasks')
            if not msg:
                break
            run_task(msg)
        task.wait()
        logs = task.format_logs()
        self.assertEqual(task.status, task.STATUS_SUCCESS)
        self.assertEqual(logs, (
            'First log from task_a.\n'
            'Second log from task_a.\n'
            'Log from task_b.'
        ))
