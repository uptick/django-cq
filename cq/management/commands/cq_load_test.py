import time
import sys

from django.core.management.base import BaseCommand, CommandError
from asgi_redis import RedisChannelLayer
from cq.decorators import task
from cq.models import Task, delay


@task
def task_j(task, a, b):
    return a + b


@task
def task_i(task):
    return 3


@task
def task_f(task):
    return task_i.delay().chain(task_j, (2,))


@task
def task_c(task, value):
    if value == 'd':
        try:
            Task.objects.get_or_create(details={'failed': 'failed'})
        except Task.MultipleObjectsReturned:
            pass
        raise Exception('Task D, should fail.')
    try:
        Task.objects.get_or_create(details={'failed': 'okay'})
    except Task.MultipleObjectsReturned:
        pass
    return value


@task
def task_a(task):
    task.subtask(task_c, ('c',))
    task.subtask(task_c, ('d',))
    return task_c.delay('e')


@task
def task_b(task):
    task.subtask(task_c, ('g',))
    task.subtask(task_c, ('h',))
    return task_f.delay()


@task
def task_trunk(task):
    task.subtask(task_a)
    task.subtask(task_b)


@task
def task_root(task):
    for ii in range(50):
        task.subtask(task_trunk)
    return 'root'


class Command(BaseCommand):
    help = 'CQ load test.'

    def add_arguments(self, parser):
        parser.add_argument('--lost', action='store_true')

    def handle(self, *args, **options):
        if options.get('lost', True):
            self.launch_lost()
        else:
            self.launch_one()

    def launch_lost(self):
        for ii in range(200):
            delay(task_root, (), {}, submit=False, status=Task.STATUS_RETRY)

    def launch_one(self):
        root = task_root.delay()
        root.wait()
        for trunk in root.subtasks.all():
            self.check_trunk(trunk)

        # Should not have committed the dummy task.
        assert not Task.objects.filter(details__failed='failed').exists(), 'Should not have committed task.'

        # Should have committed this dummy task.
        assert Task.objects.filter(details__failed='okay').exists(), 'Should have committed this one.'

    def assertEqual(self, a, b, msg):
        if a != b:
            self.stdout.write(msg)
            self.stdout.write('  {} != {}'.format(a, b))
            sys.exit(0)

    def check_trunk(self, task):
        task.wait()
        self.assertEqual(task.status, Task.STATUS_INCOMPLETE, 'trunk should be incomplete.')
        self.check_a(task, task.subtasks.get(signature__func_name='cq.management.commands.cq_load_test.task_a'))
        self.check_b(task, task.subtasks.get(signature__func_name='cq.management.commands.cq_load_test.task_b'))

    def check_a(self, trunk, task):
        task.wait()
        assert task.status == Task.STATUS_INCOMPLETE, 'task_a should be incomplete.'
        # self.assertEqual(task.result, 'e', 'task_a result should be "e"')
        self.check_c(task.subtasks.get(details__result='c'))
        self.check_d(task.subtasks.get(details__error__isnull=False))
        self.check_e(task.subtasks.get(details__result='e'))

    def check_b(self, trunk, task):
        task.wait()
        self.assertEqual(task.status, Task.STATUS_SUCCESS, 'task_b should have succeeded.')
        self.assertEqual(task.result, 5, 'task_b result should be 5')
        self.check_f(task.subtasks.get(details__result=5))
        self.check_g(task.subtasks.get(details__result='g'))
        self.check_h(task.subtasks.get(details__result='h'))

    def check_c(self, task):
        task.wait()
        assert task.result == 'c', 'task_c result should be "c"'

    def check_d(self, task):
        task.wait()
        assert task.result == None, 'task_d result should be "None"'
        self.assertEqual(task.error, 'Task D, should fail.', 'task_d should have an error')

    def check_e(self, task):
        task.wait()
        assert task.result == 'e', 'task_e result should be "e"'

    def check_f(self, task):
        task.wait()
        self.assertEqual(task.result, 5, 'task_f result should be 5')

    def check_g(self, task):
        task.wait()
        assert task.result == 'g', 'task_g result should be "g"'

    def check_h(self, task):
        task.wait()
        assert task.result == 'h', 'task_h result should be "h"'
