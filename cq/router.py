from .views import TaskViewSet


def register(router, name='cq'):
    router.register(r'cq/tasks', TaskViewSet, base_name='cqtask')
