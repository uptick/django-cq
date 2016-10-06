from django.core.management.base import CommandError
from channels.management.commands.runworker import Command as BaseCommand


class Command(BaseCommand):
    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--web-threads', action='store',
            type=int, help='Number of threads to execute.'
        )
        parser.add_argument(
            '--worker-threads', action='store',
            type=int, help='Number of threads to execute.'
        )

    def handle(self, *args, **options):
        try:
            web_threads = int(options.get('web_threads', 0))
        except TypeError:
            web_threads = 0
        try:
            wkr_threads = int(options.get('worker_threads', 0))
        except TypeError:
            wkr_threads = 0
        if (web_threads or wkr_threads):
            options['threads'] = web_threads + wkr_threads
            if web_threads:
                if options.get('thread_only_channels', None) is None:
                    options['thread_only_channels'] = []
                for ii in range(web_threads):
                    options['thread_only_channels'].append('%d,http.*' % ii)
        super().handle(*args, **options)
