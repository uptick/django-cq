from rest_framework import serializers

from .models import Task, delay


class TaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task


class CreateTaskSerializer(serializers.Serializer):
    task = serializers.CharField(max_length=128)
    args = serializers.JSONField(required=False)
    kwargs = serializers.JSONField(required=False)

    def create(self, data, *args, **kwargs):
        return delay(data['task'], data.get('args', ()), data.get('kwargs', {}))

    def to_representation(self, inst):
        return {
            'id': inst.id
        }
