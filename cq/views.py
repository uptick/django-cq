from django.shortcuts import render
from django.db import transaction
from rest_framework import viewsets, mixins

from .serializers import TaskSerializer, CreateTaskSerializer
from .models import Task


class TaskViewSet(viewsets.ModelViewSet):
    queryset = Task.objects.all()
    serializer_class = TaskSerializer

    def get_serializer(self, data):
        if getattr(self, 'creating', False):
            return CreateTaskSerializer(data=data)
        return super().get_serializer(data)

    def create(self, request, *args, **kwargs):
        self.creating = True
        with transaction.atomic():
            return super().create(request, *args, **kwargs)
