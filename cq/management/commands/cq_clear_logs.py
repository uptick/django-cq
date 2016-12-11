from django.core.management.base import BaseCommand
from cq.tasks import clear_logs


class Command(BaseCommand):
    help = 'Clear all logs from REDIS.'

    def handle(self, *args, **options):
        clear_logs()
