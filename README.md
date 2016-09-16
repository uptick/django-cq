# django-cq


## Description

An attempt to implement a distributed task queue for use with Django channels.
Modelled after RQ and Celery, complex task workflows are possible, all leveraging
the Channels machinery.

*This is very much pre-alpha.*


## Why

There are two reasons:

 1. Aiming for more fault tolerant tasks. There are many occasions where information
    regarding how tests are progressing is needed to be stored persistently. For
    important tasks, this should be stored even in the case of a Redis fault, or if
    the worker goes down.

 2. Prefer to leverage the same machinery as channels. Why double up?


## Installation

No pip package yet, as there are likely to be many changes:

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


## Tasks

Basic task usage is straight forward:

```python
@task
def send_email(task, addr):
    ...
    return 'OK'

task = send_emails.delay('dummy@dummy.org')
task.wait()
print(task.result)  # "OK"
```

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
def send_emails(task):
    ...
    for addr in email_addresses:
        task.subtask(send_email, addr)
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
def parent(task):
    task_a.delay()  # not a subtask
    task.subtask(task_b)  # subtask

parent.delay()
parent.status == Task.STATUS_WAITING  # True
# once task_b completes
parent.wait()
parent.status == Task.STATUS_COMPLETE  # True
```


## Chained Tasks

```python
@task
def calculate_something(task):
    return calc_a.delay(3).chain(add_a_to_4, (4,))
```


## Repeating Tasks

CQ comes with basic repeating tasks. There are two ways to create
repeating tasks:

 1. From the Django admin.

 2. Using a data migration.

TODO


## Time-to-live

TODO
