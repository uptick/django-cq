from django.core.management.base import BaseCommand, CommandError
from cq.tasks import retry_tasks


class Command(BaseCommand):
    help = 'Retry tasks.'

    # def add_arguments(self, parser):
    #     parser.add_argument(')

    def handle(self, *args, **options):
        retry_tasks()
