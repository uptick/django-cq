from django.db.models.signals import pre_save
from django.dispatch import receiver

from .models import RepeatingTask


@receiver(pre_save, sender=RepeatingTask)
def update_next_run(sender, instance, **kwargs):
    instance.update_next_run()
