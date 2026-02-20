import  time
from celery import shared_task
from django.utils import timezone
from datetime import datetime, timedelta
import logging

from football.models import FixtureIngestion
from football.api_client import RateLimitExceeded

logger = logging.getLogger(__name__)

# Batch sizes
H2H_BATCH_SIZE = 10
FORM_BATCH_SIZE = 5
STANDINGS_BATCH_SIZE = 3
ADVANCED_STATS_BATCH_SIZE = 50

MAX_RETRY_COUNT = 5


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def ingest_next_day_fixtures(self):
    """
    Daily task to fetch fixtures for Day 5 (the next new day).
    Runs at 2 AM every day via Celery Beat.
    
    After bootstrap, this maintains the rolling 5-day window.
    """
    try:
        # Calculate Day 5 (5 days from now)
        target_date = datetime.now() + timedelta(days=5)
        date_str = target_date.strftime('%Y-%m-%d')
        
        logger.info(f"🚀 Daily ingestion: Fetching fixtures for {date_str}")
        
        # ✅ Call regular function (not task)
        from football.tasks import fetch_and_process_day
        result = fetch_and_process_day(date_str)
        
        logger.info(f"✅ Daily ingestion complete for {date_str}: "
                   f"{result['created']} created, {result['updated']} updated")
        
        return {
            'status': 'success',
            'date': date_str,
            'total_fixtures': result['created'] + result['updated'],
            'created': result['created'],
            'updated': result['updated'],
            'skipped': result['skipped'],
            'fixture_ids': result['fixture_ids'],
            'timestamp': timezone.now().isoformat()
        }
        
    except RateLimitExceeded as exc:
        logger.warning(f"⚠️ Rate limit hit, retrying in {exc.wait_time}s")
        raise self.retry(countdown=exc.wait_time, exc=exc)
        
    except Exception as exc:
        logger.error(f"❌ Daily ingestion failed: {exc}", exc_info=True)
        raise self.retry(countdown=300, exc=exc)


@shared_task(bind=True, max_retries=3)
def process_h2h_batch(self):
    """Process a batch of fixtures that need head-to-head data. Runs every 5 minutes."""
    try:
        ingestions = FixtureIngestion.objects.filter(
            needs_h2h=True,
            h2h_retry_count__lt=MAX_RETRY_COUNT
        ).select_related(
            'fixture',
            'fixture__home_team',
            'fixture__away_team',
        )[:H2H_BATCH_SIZE]

        if not ingestions:
            return {"status": "no_work", "processed": 0}
        
        logger.info(f"🔄 Processing H2H batch: {len(ingestions)} fixtures")

        processed = 0
        rate_limited = False

        for ingestion in ingestions:
            if rate_limited:
                break

            try:
                from football.tasks import process_single_h2h
                
                result = process_single_h2h(
                    ingestion.fixture.id,
                    ingestion.fixture.home_team.id,
                    ingestion.fixture.away_team.id,
                )
                
                if result.get('status') in ['success', 'cached', 'no_data', 'no_valid_data']:
                    ingestion.needs_h2h = False
                    ingestion.h2h_processed_at = timezone.now()
                    ingestion.save(update_fields=['needs_h2h', 'h2h_processed_at', 'updated_at'])

                    # Check if fully processed
                    ingestion.refresh_from_db()
                    ingestion.check_and_mark_complete()
                    
                    processed += 1
                    
                time.sleep(0.5)
                
            except RateLimitExceeded:
                logger.warning("⚠️ Rate limit hit, stopping batch")
                ingestion.h2h_retry_count += 1
                ingestion.save(update_fields=['h2h_retry_count', 'updated_at'])
                rate_limited = True
                
            except Exception as exc:
                logger.error(f"❌ Error processing H2H for fixture {ingestion.fixture.id}: {exc}")
                ingestion.h2h_retry_count += 1
                ingestion.last_error = str(exc)[:500]
                ingestion.save(update_fields=['h2h_retry_count', 'last_error', 'updated_at'])
        
        remaining = FixtureIngestion.objects.filter(needs_h2h=True).count()
        logger.info(f"✅ H2H batch: {processed} processed, {remaining} remaining")
        
        return {
            "status": "success",
            "processed": processed,
            "remaining": remaining
        }
        
    except Exception as exc:
        logger.error(f"H2H batch task error: {exc}")
        raise self.retry(countdown=300, exc=exc)


@shared_task(bind=True, max_retries=3)
def process_form_batch(self):
    """Process team form data. Runs every 10 minutes."""
    try:
        ingestions = FixtureIngestion.objects.filter(
            needs_form=True,
            form_retry_count__lt=MAX_RETRY_COUNT
        ).select_related(
            'fixture__home_team',
            'fixture__away_team',
            'fixture__league'
        )[:FORM_BATCH_SIZE * 2]
        
        if not ingestions:
            return {"status": "no_work", "processed": 0}
        
        # Get unique teams
        teams_to_process = set()
        ingestion_map = {}  # Map team keys to ingestion objects
        
        for ing in ingestions:
            home_key = (ing.fixture.home_team.id, ing.fixture.league.season)
            away_key = (ing.fixture.away_team.id, ing.fixture.league.season)
            
            teams_to_process.add(home_key)
            teams_to_process.add(away_key)
            
            # Track which ingestions need which teams
            if ing.fixture.id not in ingestion_map:
                ingestion_map[ing.fixture.id] = {
                    'ingestion': ing,
                    'needs_home': home_key,
                    'needs_away': away_key
                }
            
            if len(teams_to_process) >= FORM_BATCH_SIZE:
                break
        
        logger.info(f"🔄 Processing form batch: {len(teams_to_process)} teams")
        
        processed = 0
        processed_teams = set()
        rate_limited = False
        
        for team_id, season in list(teams_to_process)[:FORM_BATCH_SIZE]:
            if rate_limited:
                break
                
            try:
                # ✅ Call regular function (not Celery task)
                from football.tasks import process_single_team_form
                
                result = process_single_team_form(team_id, season)
                
                if result.get('status') in ['success', 'cached', 'no_data', 'no_valid_data']:
                    processed_teams.add((team_id, season))
                    processed += 1
                    
                time.sleep(0.5)
                
            except RateLimitExceeded:
                logger.warning("⚠️ Rate limit hit, stopping batch")
                rate_limited = True
                
            except Exception as exc:
                logger.error(f"❌ Error processing form for team {team_id}: {exc}")
        
        # Update ingestion flags for fixtures with BOTH teams processed
        if processed_teams:
            for fixture_id, data in ingestion_map.items():
                home_key = data['needs_home']
                away_key = data['needs_away']
                
                if home_key in processed_teams and away_key in processed_teams:
                    ing = data['ingestion']
                    ing.needs_form = False
                    ing.form_processed_at = timezone.now()
                    ing.save(update_fields=['needs_form', 'form_processed_at', 'updated_at'])
                    
                    # Check if fully processed
                    ing.refresh_from_db()
                    ing.check_and_mark_complete()
        
        remaining = FixtureIngestion.objects.filter(needs_form=True).count()
        logger.info(f"✅ Form batch: {processed} teams processed, {remaining} fixtures remaining")
        
        return {
            "status": "success",
            "processed": processed,
            "remaining": remaining
        }
        
    except Exception as exc:
        logger.error(f"Form batch task error: {exc}")
        raise self.retry(countdown=300, exc=exc)


@shared_task(bind=True, max_retries=3)
def process_standings_batch(self):
    """Process league standings. Runs every 15 minutes."""
    try:
        ingestions = FixtureIngestion.objects.filter(
            needs_standings=True,
            standings_retry_count__lt=MAX_RETRY_COUNT
        ).select_related('fixture__league')[:STANDINGS_BATCH_SIZE * 2]
        
        if not ingestions:
            return {"status": "no_work", "processed": 0}
        
        # Get unique leagues
        leagues_to_process = set()
        league_ingestion_map = {}  # Map league keys to list of ingestions
        
        for ing in ingestions:
            league_key = (ing.fixture.league.id, ing.fixture.league.season)
            leagues_to_process.add(league_key)
            
            if league_key not in league_ingestion_map:
                league_ingestion_map[league_key] = []
            league_ingestion_map[league_key].append(ing)
            
            if len(leagues_to_process) >= STANDINGS_BATCH_SIZE:
                break
        
        logger.info(f"🔄 Processing standings batch: {len(leagues_to_process)} leagues")
        
        processed = 0
        processed_leagues = set()
        rate_limited = False
        
        for league_id, season in list(leagues_to_process)[:STANDINGS_BATCH_SIZE]:
            if rate_limited:
                break
                
            try:
                # ✅ Call regular function (not Celery task)
                from football.tasks import process_single_league_standings
                
                result = process_single_league_standings(league_id, season)
                
                if result.get('status') in ['success', 'cached', 'no_data', 'no_valid_data']:
                    processed_leagues.add((league_id, season))
                    processed += 1
                    
                time.sleep(1.0)
                
            except RateLimitExceeded:
                logger.warning("⚠️ Rate limit hit, stopping batch")
                rate_limited = True
                
            except Exception as exc:
                logger.error(f"❌ Error processing standings for league {league_id}: {exc}")
        
        # Update ingestion flags for all fixtures in processed leagues
        if processed_leagues:
            for league_key in processed_leagues:
                ingestions_to_update = league_ingestion_map.get(league_key, [])
                
                for ing in ingestions_to_update:
                    ing.needs_standings = False
                    ing.standings_processed_at = timezone.now()
                    ing.save(update_fields=['needs_standings', 'standings_processed_at', 'updated_at'])
                    
                    # Check if fully processed
                    ing.refresh_from_db()
                    ing.check_and_mark_complete()
        
        remaining = FixtureIngestion.objects.filter(needs_standings=True).count()
        logger.info(f"✅ Standings batch: {processed} leagues processed, {remaining} fixtures remaining")
        
        return {
            "status": "success",
            "processed": processed,
            "remaining": remaining
        }
        
    except Exception as exc:
        logger.error(f"Standings batch task error: {exc}")
        raise self.retry(countdown=300, exc=exc)


@shared_task(bind=True, max_retries=3)
def process_advanced_stats_batch(self):
    """
    Compute advanced stats from existing DB data (no API calls).
    Runs every 30 minutes.
    """
    try:
        # Only process fixtures that have completed basic processing
        ingestions = FixtureIngestion.objects.filter(
            needs_advanced_stats=True,
            advanced_stats_retry_count__lt=MAX_RETRY_COUNT,
            needs_h2h=False,  # Wait for basic data first
            needs_form=False,
            needs_standings=False
        ).select_related('fixture')[:ADVANCED_STATS_BATCH_SIZE]

        if not ingestions:
            return {"status": "no_work", "processed": 0}
        
        logger.info(f"🔄 Computing advanced stats: {len(ingestions)} fixtures")
        
        processed = 0
        
        for ingestion in ingestions:
            try:
                # ✅ Call regular function (not Celery task)
                from football.tasks import compute_advanced_fixture_stats
                
                compute_advanced_fixture_stats(ingestion.fixture)
                
                ingestion.needs_advanced_stats = False
                ingestion.advanced_stats_processed_at = timezone.now()
                ingestion.save(update_fields=[
                    'needs_advanced_stats',
                    'advanced_stats_processed_at',
                    'updated_at'
                ])
                
                # Check if fully processed (should always be True here)
                ingestion.refresh_from_db()
                ingestion.check_and_mark_complete()
                
                processed += 1
                
            except Exception as exc:
                logger.error(f"❌ Error computing stats for fixture {ingestion.fixture.id}: {exc}")
                ingestion.advanced_stats_retry_count += 1
                ingestion.last_error = str(exc)[:500]
                ingestion.save(update_fields=[
                    'advanced_stats_retry_count',
                    'last_error',
                    'updated_at'
                ])
        
        remaining = FixtureIngestion.objects.filter(needs_advanced_stats=True).count()
        fully_processed = FixtureIngestion.objects.filter(is_fully_processed=True).count()
        logger.info(f"✅ Advanced stats: {processed} processed, {remaining} remaining, {fully_processed} fully ready")
        
        return {
            "status": "success",
            "processed": processed,
            "remaining": remaining,
            "fully_processed_total": fully_processed
        }
        
    except Exception as exc:
        logger.error(f"Advanced stats batch task error: {exc}")
        raise self.retry(countdown=300, exc=exc)