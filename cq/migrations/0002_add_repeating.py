from django.db import migrations
from cq.tasks import maintenance
from cq.models import schedule_task


def add_repeating(apps, scema_editor):
    RepeatingTask = apps.get_model('cq.RepeatingTask')
    schedule_task(
        RepeatingTask,
        '* * * * *',
        maintenance,
        result_ttl=30,
        coalesce=False
    )


class Migration(migrations.Migration):
    dependencies = [
        ('cq', '0001_initial')
    ]
    operations = [
        migrations.RunPython(add_repeating, reverse_code=migrations.RunPython.noop)
    ]
