import logging

from .models import Task
from .task import SerialTask


logger = logging.getLogger('cq')


def run_task(message):
    task_id = message['task_id']
    task = Task.objects.get(id=task_id)
    logger.info('Running task "{}".'.format(task))
    try:
        result = task.start()
    except Exception as err:
        task.failure(err)
    else:
        if not isinstance(result, Task):
            if isinstance(result, SerialTask):
                result = result.result
            task.success(result)
        else:
            task.waiting()
