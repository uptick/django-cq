from django.shortcuts import render
from rest_framework import viewsets

from .serializers import TaskSerializer


class TaskViewSet(viewsets.ModelViewSet):
    queryset = Tasks.objects.all()
    serializer_class = TaskSerializer
