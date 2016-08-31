from django.contrib import admin
from django import forms

from .models import Task, RepeatingTask
from .task import FuncNameWidget


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'submitted', 'func_name', 'status')
    list_filter = ('status',)


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
    list_display = ('func_name', 'crontab', 'last_run', 'next_run')
