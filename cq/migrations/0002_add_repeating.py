from django.db import migrations
from cq.tasks import clean_up, retry_tasks, check_lost
from cq.models import schedule_task


def add_repeating(apps, scema_editor):
    RepeatingTask = apps.get_model('cq.RepeatingTask')
    schedule_task(
        RepeatingTask,
        '* * * * *',
        clean_up,
        result_ttl=30
    )
    schedule_task(
        RepeatingTask,
        '* * * * *',
        retry_tasks,
        result_ttl=30
    )
    schedule_task(
        RepeatingTask,
        '* * * * *',
        check_lost,
        result_ttl=30
    )


class Migration(migrations.Migration):
    dependencies = [
        ('cq', '0001_initial')
    ]
    operations = [
        migrations.RunPython(add_repeating, reverse_code=migrations.RunPython.noop)
    ]
