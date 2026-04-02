from celery import shared_task
from django.utils import timezone
from django.core.cache import cache
from datetime import timedelta
import logging

from football.models import Fixture, FixtureIngestion,  HeadToHeadMatch


logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=2)
def cleanup_old_fixtures(self):
    """
    Delete fixtures from yesterday (already passed).
    Runs daily at 6 PM.
    
    Logic:
    - Today is Day 0
    - We keep: Day 0, 1, 2, 3, 4 (5 days total)
    - We delete: Day -1 (yesterday)
    
    This maintains the rolling 5-day window.
    """
    try:
        # Calculate yesterday's date
        yesterday = timezone.now().date() - timedelta(days=1)
        
        logger.info(f"🗑️ Starting cleanup for fixtures from {yesterday}")
        
        # Find fixtures from yesterday
        old_fixtures = Fixture.objects.filter(
            date__date=yesterday
        )
        
        count = old_fixtures.count()
        
        if count == 0:
            logger.info(f"✓ No fixtures to clean up for {yesterday}")
            return {
                'status': 'success',
                'date': str(yesterday),
                'deleted': 0,
                'message': 'No fixtures found'
            }
        
        # Get fixture IDs before deletion (for logging + cache bust)
        deleted_ids = list(old_fixtures.values_list('id', flat=True))

        # Bust fixture_stats page cache before deleting so stale entries don't linger
        for fid in deleted_ids:
            cache.delete(f'fixture_stats_{fid}')

        # Delete fixtures (cascade will delete FixtureIngestion, H2H, Form, etc.)
        old_fixtures.delete()
        
        logger.info(f"✅ Deleted {count} fixtures from {yesterday}")
        logger.debug(f"Deleted fixture IDs: {deleted_ids[:10]}...")
        
        # Also cleanup orphaned data (extra safety)
        cleanup_orphaned_data()
        
        return {
            'status': 'success',
            'date': str(yesterday),
            'deleted': count,
            'fixture_ids': deleted_ids
        }
        
    except Exception as exc:
        logger.error(f"❌ Cleanup task failed: {exc}", exc_info=True)
        raise self.retry(countdown=300, exc=exc)


def cleanup_orphaned_data():
    """
    Clean up any orphaned related data.
    Extra safety measure.
    """
    
    # Delete H2H matches with no fixture
    orphaned_h2h = HeadToHeadMatch.objects.filter(fixture__isnull=True)
    h2h_count = orphaned_h2h.count()
    if h2h_count > 0:
        orphaned_h2h.delete()
        logger.info(f"Cleaned up {h2h_count} orphaned H2H records")
    
    # Delete ingestion records with no fixture
    orphaned_ingestion = FixtureIngestion.objects.filter(fixture__isnull=True)
    ing_count = orphaned_ingestion.count()
    if ing_count > 0:
        orphaned_ingestion.delete()
        logger.info(f"Cleaned up {ing_count} orphaned ingestion records")

