# django-cq


## Description

An attempt to implement a distributed task queue for use with Django channels.
Modelled after RQ and Celery, complex task workflows are possible, all leveraging
the Channels machinery.


## Why

There are three reasons:

 1. Aiming for more fault tolerant tasks. There are many occasions where information
    regarding how tests are progressing is needed to be stored persistently. For
    important tasks, this should be stored even in the case of a Redis fault, or if
    the worker goes down.

 2. Prefer to leverage the same machinery as channels.

 3. Would like to have a little extra functionality surrounding subtasks that didn't
    seem to be available via Celery or RQ.


## Limitations

There are two limitations involved:

  * REDIS must be used as the Django cache.

  * `asgi_redis` must be used as the channel backend.

There is work being done to remove these restrictions.


## Installation

Use pip if you can:

```bash
pip install cq
```

or live on the edge:

```bash
pip install -e https://github.com/furious-luke/django-cq.git#egg=django-cq
```

Add the package to your settings file:

```python
INSTALLED_APPS = [
   'cq',
   ...
]
```

And include the routing information in your channel routing list:

```python
channel_routing = [
    include('cq.routing.channel_routing'),
    ...
]
```

You'll need to migrate to include the models:

```bash
./manage.py migrate
```

You'll most likely want to create a new channel layer for your CQ
tasks. The default layer has a short time-to-live on the channel messages,
which causes slightly long running tasks to kill off any queued messages.
Update your settings file to include the following:

```python
CHANNEL_LAYERS = {
    'default': {
        ...
    },
    'long': {
        'BACKEND': 'asgi_redis.RedisChannelLayer',
        'CONFIG': {
            'hosts': [REDIS_URL],
            'expiry': 1800,
            'channel_capacity': {
                'cq-tasks': 1000
            }
        },
        'ROUTING': 'path.to.your.channels.channel_routing',
    },
}

CQ_CHANNEL_LAYER = 'long'
```


## Tasks

Basic task usage is straight forward:

```python
@task
def send_email(cqt, addr):
    ...
    return 'OK'

task = send_emails.delay('dummy@dummy.org')
task.wait()
print(task.result)  # "OK"
```

Here, `cqt` is the task representation for the `send_email` task. This
can be used to launch subtasks, chain subsequent tasks, amongst other
things.

Tasks may also be run in serial by just calling them:

```python
result = send_email('dummy@dummy.org')
print(result)  # "OK"
```


## Subtasks

For more complex workflows, subtasks may be launched from within
parent tasks:

```python
@task
def send_emails(cqt):
    ...
    for addr in email_addresses:
        cqt.subtask(send_email, addr)
    ...
    return 'OK'

task = send_emails.delay()
task.wait()
print(task.result)  # "OK"
```

The difference between a subtask and another task launched using `delay` from
within a task is that the parent task of a subtask will not be marked as complete
until all subtasks are also complete.

```python
from cq.models import Task

@task
def parent(cqt):
    task_a.delay()  # not a subtask
    cqt.subtask(task_b)  # subtask

parent.delay()
parent.status == Task.STATUS_WAITING  # True
# once task_b completes
parent.wait()
parent.status == Task.STATUS_COMPLETE  # True
```


## Chained Tasks

TODO

```python
@task
def calculate_something(cqt):
    return calc_a.delay(3).chain(add_a_to_4, (4,))
```


## Non-atomic Tasks

By default every CQ task is atomic; no changes to the database will persist
unless the task finishes without an exception. If you need to keep changes to
the database, even in the event of an error, then use the `atomic` flag:

```python
@task(atomic=False)
def unsafe_task(cqt):
    pass
```


## Logging

For longer running tasks it's useful to be able to access an ongoing log
of the task's progress. CQ tasks have a `log` method to send logging
messages to both the standard Django log streams, and also cache them on
the running task.

```python
@task
def long_task(cqt):
    cqt.log('standard old log')
    cqt.log('debugging log', logging.DEBUG)
```

If the current task is a subtask, the logs will go to the parent.
This way there is a central task (the top-level task) which can be used
to monitor the progress and status of a network of sub and chained tasks.

### Performance

Due to the way logs are handled there can be issues with performance
with a lot of frequent log messages. There are two ways to prevent this.

Reduce the frequency of logs by setting `publish` to `False` on as many
log calls as you can. This will cache the logs locally and store them
on the next `publish=True` call.

```python
@task
def long_task(cqt):
    for ii in range(100):
        cqt.log('iteration %d' % ii, publish=False)
    cqt.log('done')  # publish=True
```

Secondly, reducing the volume of logs may be accomplished by limiting the
number of log lines that are kept. The `limit` option specifies this. The
following will only keep 10 of the logged iterations:

```python
@task
def long_task(cqt):
    for ii in range(100):
        cqt.log('iteration %d' % ii, publish=False)
    cqt.log('done', limit=10)
```

## Time-to-live

TODO

## Repeating Tasks

CQ comes with robust repeating tasks. There are two ways to create
repeating tasks:

 1. From the Django admin.

 2. Using a data migration.

From the admin, click into `cq` and `repeating tasks`. From there you
can create a new repeating task, specifying the background task to call,
and a CRON time for repetition.

To create a repeating task from a migration, use the helper function
`schedule_task`.

```python
from django.db import migrations
from cq.models import schedule_task

from myapp.tasks import a_task


def add_repeating(apps, scema_editor):
    RepeatingTask = apps.get_model('cq.RepeatingTask')
    schedule_task(
        RepeatingTask,
        '* * * * *',
        a_task
    )


class Migration(migrations.Migration):
    operations = [
        migrations.RunPython(add_repeating, reverse_code=migrations.RunPython.noop)
    ]
```


### Coalescing

# TODO
