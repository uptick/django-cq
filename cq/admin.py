from django.contrib import admin

from .models import Task, RepeatingTask


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    pass


@admin.register(RepeatingTask)
class RepeatingTaskAdmin(admin.ModelAdmin):
    pass
