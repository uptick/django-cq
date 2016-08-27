from django.contrib import admin

from .models import Task, RepeatingTask


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'submitted', 'func_name', 'status')
    list_filter = ('status',)


@admin.register(RepeatingTask)
class RepeatingTaskAdmin(admin.ModelAdmin):
    list_display = ('func_name', 'crontab', 'last_run', 'next_run')
