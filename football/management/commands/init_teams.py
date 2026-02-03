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
            help='Initial wait time in seconds when queue limit is reached'
        )
        parser.add_argument(
            '--max-retries',
            type=int,
            default=3,
            help='Maximum number of retries for connection/unknown errors only'
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
        countries_names = self._get_countries_names(options.get('start_limit'), options.get('end_limit'))
        batch_size = options['countries_per_task']
        dry_run = options['dry_run']
        max_queue = options['max_queue']
        initial_wait_time = options['wait_time']
        max_retries = options['max_retries']

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
                
                # Process batch with guaranteed completion (except for fatal errors)
                batch_result = self._queue_batch_with_persistence(
                    batch_names, redis, max_queue, initial_wait_time, max_retries, progress.n, total_batches
                )
                
                if batch_result:
                    successful_batches += 1
                else:
                    failed_batches += 1
                
                progress.update(1)

        # Summary
        self.stdout.write(
            self.style.SUCCESS(
                f"\nProcessing complete:\n"
                f"- Successful batches: {successful_batches}\n"
                f"- Failed batches: {failed_batches}\n"
                f"- Total batches: {total_batches}"
            )
        )

    def _queue_batch_with_persistence(self, batch_names: List[str], redis, max_queue: int, 
                                    initial_wait_time: int, max_retries: int, 
                                    batch_num: int, total_batches: int) -> bool:
        """
        Queue a batch with persistence - will not give up due to queue size limits.
        Only fails on connection errors or unknown errors after retries.
        
        Returns True if successfully queued, False if permanently failed.
        """
        retry_count = 0
        wait_time = initial_wait_time
        
        # Ensure batch_names is a list
        if not isinstance(batch_names, list):
            batch_names = list(batch_names)

        while True:
            try:
                # Check current queue size
                current_queue_size = redis.llen(self.CELERY_QUEUE_NAME)
                
                # If queue is full, wait indefinitely with exponential backoff
                if current_queue_size >= max_queue:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Queue full ({current_queue_size}/{max_queue}). "
                            f"Waiting {wait_time}s for batch {batch_num}/{total_batches}..."
                        )
                    )
                    time.sleep(wait_time)
                    # Exponential backoff with cap at 60 seconds
                    wait_time = min(wait_time * 1.5, 60)
                    continue  # Keep trying - NO BATCH LEFT BEHIND for queue size

                # Queue has space - attempt to queue the task
                populate_teams_task.delay(batch_names)
                
                logger.info(f"Successfully queued batch {batch_num}: {batch_names}")
                self.stdout.write(
                    self.style.SUCCESS(
                        f"✓ Queued batch {batch_num}/{total_batches} "
                        f"(Queue size: ~{current_queue_size + 1})"
                    )
                )
                return True  # SUCCESS!

            except ConnectionError as e:
                # Connection errors are serious - retry with limit
                retry_count += 1
                logger.error(f"Redis connection error for batch {batch_num} (attempt {retry_count}): {e}")
                self.stderr.write(
                    self.style.ERROR(
                        f"Redis connection error for batch {batch_num} (attempt {retry_count}/{max_retries}): {e}"
                    )
                )
                
                if retry_count >= max_retries:
                    self.stderr.write(
                        self.style.ERROR(f"✗ FATAL: Max connection retries reached for batch {batch_num}")
                    )
                    return False  # PERMANENT FAILURE
                
                # Wait before retry with exponential backoff
                backoff_time = initial_wait_time * (2 ** (retry_count - 1))
                self.stdout.write(f"Retrying connection in {backoff_time}s...")
                time.sleep(backoff_time)
                continue

            except TimeoutError as e:
                # Timeout errors - retry with limit
                retry_count += 1
                logger.error(f"Celery timeout error for batch {batch_num} (attempt {retry_count}): {e}")
                self.stderr.write(
                    self.style.ERROR(
                        f"Celery timeout for batch {batch_num} (attempt {retry_count}/{max_retries}): {e}"
                    )
                )
                
                if retry_count >= max_retries:
                    self.stderr.write(
                        self.style.ERROR(f"✗ FATAL: Max timeout retries reached for batch {batch_num}")
                    )
                    return False  # PERMANENT FAILURE
                
                # Wait before retry
                backoff_time = initial_wait_time * (2 ** (retry_count - 1))
                time.sleep(backoff_time)
                continue

            except Exception as e:
                # Unknown errors - retry with limit
                retry_count += 1
                logger.exception(f"Unknown error queuing batch {batch_num} (attempt {retry_count})")
                self.stderr.write(
                    self.style.ERROR(
                        f"Unknown error for batch {batch_num} (attempt {retry_count}/{max_retries}): {str(e)}"
                    )
                )
                
                if retry_count >= max_retries:
                    self.stderr.write(
                        self.style.ERROR(f"✗ FATAL: Max retries reached for unknown error on batch {batch_num}")
                    )
                    return False  # PERMANENT FAILURE
                
                # Wait before retry
                backoff_time = initial_wait_time * (2 ** (retry_count - 1))
                time.sleep(backoff_time)
                continue

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