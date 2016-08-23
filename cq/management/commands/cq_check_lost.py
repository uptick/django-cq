from django.core.management.base import BaseCommand, CommandError
from cq.tasks import check_lost


class Command(BaseCommand):
    help = 'Check for lost tasks.'

    # def add_arguments(self, parser):
    #     parser.add_argument(')

    def handle(self, *args, **options):
        check_lost()
