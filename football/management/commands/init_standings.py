from django.core.management.base import BaseCommand
from django_redis import get_redis_connection
from football.models import League
from football.tasks import populate_standings_task
from tqdm import tqdm
import logging
from typing import List
from celery.exceptions import TimeoutError
import time

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Initialize database with standings of leagues'
    CELERY_QUEUE_NAME = "celery"

    def add_arguments(self, parser):
        parser.add_argument(
            '--leagues-per-task',
            type=int,
            default=5,
            help='Number of leagues to process per celery task'
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

    def handle(self, *args, **options):
        leagues_ids = self._get_leagues_ids()
        batch_size = options['leagues_per_task']
        dry_run = options['dry_run']
        max_queue = options['max_queue']
        wait_time = options['wait_time']
        

        
       

        self.stdout.write(
            self.style.SUCCESS(f"Found {len(leagues_ids)} leagues to process")
        )

        if dry_run:
            self.stdout.write(
                self.style.WARNING("Dry run mode - no tasks will be created")
            )

        total_batches = (len(leagues_ids) + batch_size - 1) // batch_size
        redis = get_redis_connection()
        
        

        successful_batches = 0
        failed_batches = 0

        with tqdm(total=total_batches, desc='Queueing league batches') as progress:
            for i in range(0, len(leagues_ids), batch_size):
                batch_ids = leagues_ids[i:i + batch_size]

                if not batch_ids:
                    progress.update(1)
                    continue

                if dry_run:
                    self.stdout.write(
                        f"DRY RUN: Would queue batch for league IDs: {batch_ids}"
                    )
                    progress.update(1)
                    successful_batches += 1
                    continue

                try:
                    # Check current queue size using llen for better accuracy
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

                    # Ensure batch_ids is a list (it already is from slicing)
                    if not isinstance(batch_ids, list):
                        batch_ids = list(batch_ids)

                    # Queue the task
                    populate_standings_task.delay(batch_ids)

                    # Small delay to prevent Redis hammering (optional)
                    time.sleep(0.1)
                    
                    # Increment after successful queuing
                    successful_batches += 1
                    progress.update(1)
                    
                    logger.info(f"Queued batch {progress.n}: {batch_ids}")
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Queued batch {progress.n}/{total_batches} "
                            f"(Queue size: {current_queue_size + 1})"
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

    def _get_leagues_ids(self) -> List[int]:
        """Fetch all league IDs as a list for consistent ordering."""
        try:
            return list(League.objects.only('id').values_list('id', flat=True))
        except Exception as e:
            logger.error(f"Failed to fetch league IDs: {e}")
            self.stderr.write(
                self.style.ERROR(f"Failed to fetch league IDs: {e}")
            )
            return []