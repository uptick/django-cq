from channels.routing import route

from .consumers import run_task


channel_routing = [
    route('cq-tasks', run_task)
]
