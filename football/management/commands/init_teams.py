from django.core.management.base import BaseCommand
from django.db import transaction
from football.models import Country
from football.tasks import populate_teams_task
from tqdm import tqdm 
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Initialize the database with Teams from external API'

    def add_arguments(self, parser):
        parser.add_argument(
            '--countries-per-task',
            type=int,
            default=5,
            help='Number of countries to process per Celery task'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Simulate the command without actually creating tasks'
        )
    
    def handle(self, *args, **kwargs):
        countries_map = self._get_countries_map()
        batch_size = kwargs['countries_per_task']
        dry_run = kwargs['dry_run']

        self.stdout.write(
            self.style.SUCCESS(f"Found {len(countries_map)} countries to process")
        )

        if dry_run:
            self.stdout.write(
                self.style.WARNING("Dry run mode - no tasks will be actually created")
            )

        country_names = list(countries_map.keys())
        total_batches = (len(country_names) + batch_size - 1) // batch_size

        with tqdm(total=total_batches, desc="Queueing country batches") as progress:
            for i in range(0, len(country_names), batch_size):
                batch_names = country_names[i:i+batch_size]
                

                if not dry_run:
                    try:
                        populate_teams_task.delay(batch_names)
                        progress.update(1)
                        self.stdout.write(f"Queued batch {progress.n}")
                    except Exception as e:
                        self.stderr.write(
                            self.style.ERROR(f"Failed to queue batch {progress.n}: {str(e)}")
                        )
                else:
                    progress.update(1)
                    self.stdout.write(f"Would queue batch for: {', '.join(batch_names)}")

    def _get_countries_map(self):
        """Return a mapping of country names to Country objects"""
        return {c.name: c for c in Country.objects.all()[:7]}