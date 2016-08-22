# django-cq


## Description

An attempt implement a distributed task queue for use with Django channels.


## Why

There are two reasons:

 1. Aiming for more fault tolerant tasks.

 2. Prefer to leverage the same machinery as channels; why double up?


## Installation

Setup channels.

```bash
pip install django-cq
```

```python
INSTALLED_APPS = [
   'cq',
   ...
]
```

```python
channel_routing = [
    include('cq.routing.channel_routing'),
    ...
]
```


## Usage

```python
@task
def send_emails(task):
    ...
```

```python
@task
def send_emails(task):
    ...
    for addr in email_addresses:
        task.subtask(send_email, addr)
    ...
```

```python
@task
def calculate_something(task):
    return calc_a.delay(3).chain(add_a_to_4, (4,))
```
