from .consumers import CQConsumer

channel_routing = {
    'cq-task': CQConsumer
}
