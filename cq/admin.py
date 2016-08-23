from django.contrib import admin

from .models import Task, RepeatingTask


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'func_name', 'status')


@admin.register(RepeatingTask)
class RepeatingTaskAdmin(admin.ModelAdmin):
    list_display = ('func_name', 'crontab', 'last_run', 'next_run')
