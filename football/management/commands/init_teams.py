from django.core.management.base import BaseCommand
from django.db import transaction
from football.models import Country
from django_redis import get_redis_connection
from football.tasks import populate_teams_task
from tqdm import tqdm 
import logging
from typing import List
from celery.exceptions import TimeoutError
import time

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Initialize the database with Teams from external API'
    CELERY_QUEUE_NAME = "celery"

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
        parser.add_argument(
            '--max-queue',
            type=int,
            default=100,
            help='Maximum number of tasks to queue at once'
        )
        parser.add_argument(
            '--wait-time',
            type=int,
            default=10,
            help='Wait time in seconds when queue limit is reached'
        )
        parser.add_argument(
            '--start-limit',
            type=int,
            help='Limit number of countries to process (useful for testing)'
        )
        parser.add_argument(
            '--end-limit',
            type=int,
            help='Limit number of countries to process (useful for testing)'
        )
    
    def handle(self, *args, **options):
        countries_names = self._get_countries_names(options.get('start_limit'),options.get('end_limit'))
        batch_size = options['countries_per_task']
        dry_run = options['dry_run']
        max_queue = options['max_queue']
        wait_time = options['wait_time']

        self.stdout.write(
            self.style.SUCCESS(f"Found {len(countries_names)} countries to process")
        )

        if dry_run:
            self.stdout.write(
                self.style.WARNING("Dry run mode - no tasks will be created")
            )

        total_batches = (len(countries_names) + batch_size - 1) // batch_size
        redis = get_redis_connection()
        
        successful_batches = 0
        failed_batches = 0

        with tqdm(total=total_batches, desc="Queueing country batches") as progress:
            for i in range(0, len(countries_names), batch_size):
                batch_names = countries_names[i:i + batch_size]
                
                if not batch_names:
                    progress.update(1)
                    continue

                if dry_run:
                    self.stdout.write(
                        f"DRY RUN: Would queue batch for countries: {batch_names}"
                    )
                    progress.update(1)
                    successful_batches += 1
                    continue

                try:
                    # Check current queue size using llen for accuracy
                    current_queue_size = redis.llen(self.CELERY_QUEUE_NAME)
                    
                    if current_queue_size >= max_queue:
                        self.stdout.write(
                            self.style.WARNING(
                                f"Queue limit reached ({current_queue_size}/{max_queue}). "
                                f"Waiting {wait_time}s..."
                            )
                        )
                        time.sleep(wait_time)
                        continue  # Retry this batch

                    # Ensure batch_names is a list (it already is from slicing)
                    if not isinstance(batch_names, list):
                        batch_names = list(batch_names)

                    # Queue the task
                    populate_teams_task.delay(batch_names)
                    
                    successful_batches += 1
                    progress.update(1)
                    
                    logger.info(f"Queued batch {progress.n}: {batch_names}")
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Queued batch {progress.n}/{total_batches} "
                            f"(Queue size: ~{current_queue_size + 1})"
                        )
                    )

                except (ConnectionError, TimeoutError) as e:
                    failed_batches += 1
                    logger.error(f"Celery connection error for batch {progress.n}: {e}")
                    self.stderr.write(
                        self.style.ERROR(f"Celery connection error: {e}")
                    )
                    # Decide whether to break or continue based on error severity
                    if isinstance(e, ConnectionError):
                        break  # Critical error, stop processing
                    else:
                        continue  # Timeout, try next batch

                except Exception as e:
                    failed_batches += 1
                    logger.exception(f"Failed to queue batch {progress.n}")
                    self.stderr.write(
                        self.style.ERROR(f"Failed to queue batch {progress.n}: {str(e)}")
                    )
                    continue

        # Summary
        self.stdout.write(
            self.style.SUCCESS(
                f"\nProcessing complete:\n"
                f"- Successful batches: {successful_batches}\n"
                f"- Failed batches: {failed_batches}\n"
                f"- Total batches: {total_batches}"
            )
        )

    def _get_countries_names(self, start_limit=None, end_limit=None) -> List[str]:
        """Fetch country names as a list for consistent ordering."""
        try:
            queryset = Country.objects.only('name').values_list('name', flat=True)
            
            # Apply slicing if any limit is specified
            if start_limit is not None or end_limit is not None:
                queryset = queryset[start_limit:end_limit]
                
            return list(queryset)
        except Exception as e:
            logger.error(f"Failed to fetch country names: {e}")
            self.stderr.write(
                self.style.ERROR(f"Failed to fetch country names: {e}")
            )
            return []