from .views import TaskViewSet


def register(router, name='cqtasks'):
    router.register(r'cqtasks', TaskViewSet)
