from django.db import models


class TaskManager(models.Manager):
    def active(self, **kwargs):
        return self.filter(status__in=self.model.STATUS_ACTIVE, **kwargs)
