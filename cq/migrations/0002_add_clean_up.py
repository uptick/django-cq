from django.db import migrations
from cq.tasks import clean_up


def add_clean_up(apps, scema_editor):
    RepeatingTask = apps.get_model('cq.RepeatingTask')
    RepeatingTask.schedule(
        '0 0 * * *',
        clean_up
    )


class Migration(migrations.Migration):
    dependencies = [
        ('cq', '0001_initial')
    ]
    operations = [
        migrations.RunPython(add_clean_up, reverse_code=migrations.RunPython.noop)
    ]
