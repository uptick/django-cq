import datetime

from django.core.management.base import BaseCommand
from django.utils import timezone

from ...models import Task


class Command(BaseCommand):
    help = 'Perform maintenance on CQ tasks.'

    def add_arguments(self, parser):
        parser.add_argument('--clear', action='store_true')
        parser.add_argument('--failed', action='store_true')
        parser.add_argument('--pending', action='store_true')
        parser.add_argument('--lost', action='store_true')
        parser.add_argument(
            '--prior',
            help='number of days ago prior to which the search occurs'
        )
        parser.add_argument('--dry-run', action='store_true')

    def handle(self, *args, **options):
        if options['clear']:
            self.clear(options)

    def clear(self, options):
        query = {}

        statuses = []
        if options['failed']:
            statuses.append(Task.STATUS_FAILURE)
        if options['pending']:
            statuses.append(Task.STATUS_PENDING)
        if options['lost']:
            statuses.append(Task.STATUS_LOST)
        query['status__in'] = statuses

        if options.get('prior', None):
            query['submitted__lte'] = (
                timezone.now()
                - datetime.timedelta(days=int(options['prior']))
            )

        objs = Task.objects.filter(**query)
        self.stdout.write('Removing {} objects.'.format(len(objs)))
        if not options['dry_run']:
            objs.delete()
