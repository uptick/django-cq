from django import forms
from django.contrib import admin

from .models import RepeatingTask, Task
from .task import FuncNameWidget


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'submitted', 'func_name', 'status', )
    list_filter = ('status', )


class RepeatingTaskAdminForm(forms.ModelForm):
    class Meta:
        model = RepeatingTask
        fields = '__all__'
        widgets = {
            'func_name': FuncNameWidget
        }


@admin.register(RepeatingTask)
class RepeatingTaskAdmin(admin.ModelAdmin):
    form = RepeatingTaskAdminForm
    list_display = ('func_name', 'args', 'coalesce', 'crontab', 'last_run', 'next_run', )
