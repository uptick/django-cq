from django.test import override_settings
from channels.tests import (
    TransactionChannelTestCase
)

from ..decorators import task
from ..consumers import run_task
from ..models import Task
from .test_base import SilentMixin


@task
def task_a(task, force_chain=True):
    task.subtask(task_b).chain(task_e, force_chain=force_chain).chain(task_h, force_chain=force_chain)
    return 'a'


@task
def task_b(task):
    return task.subtask(task_c).chain(task_d)


@task
def task_c(task):
    return 'c'


@task
def task_d(task):
    raise Exception('d')


@task
def task_e(task):
    return task.subtask(task_f).chain(task_g)


@task
def task_f(task):
    return 'f'


@task
def task_g(task):
    return 'g'


@task
def task_h(task):
    return task.subtask(task_i).chain(task_j)


@task
def task_i(task):
    return 'i'


@task
def task_j(task):
    raise Exception('j')


@override_settings(CQ_SERIAL=False, CQ_CHANNEL_LAYER='default')
class ForceChainTestCase(SilentMixin, TransactionChannelTestCase):
    def get_task(self, func_name):
        for task in Task.objects.all():
            if task.func_name == func_name:
                return task
        return None

    def test_force(self):
        task = task_a.delay()
        try:
            while 1:
                run_task(self.get_next_message('cq-tasks', require=True))
        except:
            pass
        task.wait()

        self.assertEqual(task.status, 'I')
        self.assertEqual(task.subtasks.all().count(), 3)
        self.assertIn('cq.tests.test_force_chain.task_b', [t.func_name for t in task.subtasks.all()])
        self.assertIn('cq.tests.test_force_chain.task_e', [t.func_name for t in task.subtasks.all()])
        self.assertIn('cq.tests.test_force_chain.task_h', [t.func_name for t in task.subtasks.all()])

        task_b = self.get_task('cq.tests.test_force_chain.task_b')
        task_c = self.get_task('cq.tests.test_force_chain.task_c')
        task_d = self.get_task('cq.tests.test_force_chain.task_d')
        self.assertEqual(task_b.status, 'I')
        self.assertEqual(task_c.status, 'S')
        self.assertEqual(task_d.status, 'F')

        task_e = self.get_task('cq.tests.test_force_chain.task_e')
        task_f = self.get_task('cq.tests.test_force_chain.task_f')
        task_g = self.get_task('cq.tests.test_force_chain.task_g')
        self.assertEqual(task_e.status, 'S')
        self.assertEqual(task_f.status, 'S')
        self.assertEqual(task_g.status, 'S')

        task_h = self.get_task('cq.tests.test_force_chain.task_h')
        task_i = self.get_task('cq.tests.test_force_chain.task_i')
        task_j = self.get_task('cq.tests.test_force_chain.task_j')
        self.assertEqual(task_h.status, 'I')
        self.assertEqual(task_i.status, 'S')
        self.assertEqual(task_j.status, 'F')

    def test_no_force(self):
        task = task_a.delay(force_chain=False)
        try:
            while 1:
                run_task(self.get_next_message('cq-tasks', require=True))
        except:
            pass
        task.wait()

        self.assertEqual(task.status, 'I')
        self.assertEqual(task.subtasks.all().count(), 3)
        self.assertIn('cq.tests.test_force_chain.task_b', [t.func_name for t in task.subtasks.all()])
        self.assertIn('cq.tests.test_force_chain.task_e', [t.func_name for t in task.subtasks.all()])
        self.assertIn('cq.tests.test_force_chain.task_h', [t.func_name for t in task.subtasks.all()])

        task_b = self.get_task('cq.tests.test_force_chain.task_b')
        task_c = self.get_task('cq.tests.test_force_chain.task_c')
        task_d = self.get_task('cq.tests.test_force_chain.task_d')
        self.assertEqual(task_b.status, 'I')
        self.assertEqual(task_c.status, 'S')
        self.assertEqual(task_d.status, 'F')

        task_e = self.get_task('cq.tests.test_force_chain.task_e')
        task_f = self.get_task('cq.tests.test_force_chain.task_f')
        task_g = self.get_task('cq.tests.test_force_chain.task_g')
        self.assertEqual(task_e.status, 'P')
        self.assertEqual(task_f, None)
        self.assertEqual(task_g, None)

        task_h = self.get_task('cq.tests.test_force_chain.task_h')
        task_i = self.get_task('cq.tests.test_force_chain.task_i')
        task_j = self.get_task('cq.tests.test_force_chain.task_j')
        self.assertEqual(task_h.status, 'P')
        self.assertEqual(task_i, None)
        self.assertEqual(task_j, None)
