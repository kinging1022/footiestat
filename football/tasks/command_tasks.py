from celery import shared_task
from football.models import Team, Country 
from celery.exceptions import Retry
from django.core.exceptions import ValidationError

import logging


logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=None)  # Disable automatic limits
def populate_teams_task(self, country_names, real_retries=0, rate_limit_retries=0):
    """
    The KEY is in the exception handling - we catch specific exception types
    and increment the appropriate counter based on WHAT TYPE of error occurred
    """
    MAX_REAL_RETRIES = 3
    MAX_RATE_LIMIT_RETRIES = 20
    
    logger.info(f"Task start - Real: {real_retries}, Rate: {rate_limit_retries}")
    
    try:
        countries = Country.objects.filter(name__in=country_names).in_bulk(field_name='name')
        
        for country_name in country_names:
            country = countries.get(country_name)
            if not country:
                logger.warning(f"Country {country_name} not found in database")
                continue
                
            _process_single_country(self, country_name, country, real_retries, rate_limit_retries)
            
    except Retry:
        # Preserve explicit retries from _process_single_country
        raise
    except Exception as exc:
        # THIS IS A REAL ERROR (not rate limit) - increment real_retries
        if real_retries >= MAX_REAL_RETRIES:
            logger.error(f"Max real retries ({MAX_REAL_RETRIES}) exceeded")
            return {'status': 'failed', 'reason': 'max_real_retries'}
            
        logger.error(f"Real error in batch (attempt {real_retries + 1}): {exc}")
        # Increment ONLY real_retries, keep rate_limit_retries same
        raise self.retry(args=[country_names, real_retries + 1, rate_limit_retries], exc=exc)


def _process_single_country(task, country_name, country, real_retries, rate_limit_retries):
    """
    This function determines which counter to increment based on the EXCEPTION TYPE
    """
    MAX_REAL_RETRIES = 3
    MAX_RATE_LIMIT_RETRIES = 20
    
    try:
        from football.api_client import get_team_details, RateLimitExceeded

        existing_country_count = Team.objects.filter(country=country).count()

        if existing_country_count > 0:
            logger.error(f"Country {country_name} already has teams in DB.")
            return
        
        # Make API call
        response = get_team_details(country=country_name)
        
        if not response or not isinstance(response.get('response'), list):
            logger.error(f"Invalid API response for {country_name}")
            # THIS IS A REAL ERROR - will increment real_retries
            if real_retries >= MAX_REAL_RETRIES:
                logger.error(f"Max real retries exceeded for {country_name}")
                return
            raise Exception("Invalid API response")  # This will be caught as real error
        
        # Process teams
        teams = []
        for team_data in response['response']:
            team_info = team_data.get('team', {})
            if not team_info.get('id'):
                continue
                
            teams.append(Team(
                id=team_info['id'],
                name=team_info.get('name'),
                short_name=team_info.get('code'),
                logo=team_info.get('logo'),
                country=country,
                national=team_info.get('national', False)
            ))
        
        if teams:
            Team.objects.bulk_create(teams, batch_size=500, ignore_conflicts=True)
        
        logger.info(f"✓ {country_name}: {len(teams)} teams created")
        
    except RateLimitExceeded as exc:
        # THIS IS THE KEY: We catch RateLimitExceeded specifically
        # This means we increment rate_limit_retries, NOT real_retries
        if rate_limit_retries >= MAX_RATE_LIMIT_RETRIES:
            logger.error(f"Max rate limit retries ({MAX_RATE_LIMIT_RETRIES}) exceeded for {country_name}")
            return
            
        logger.warning(f"Rate limited for {country_name} (rate retry #{rate_limit_retries + 1})")
        
        # Increment ONLY rate_limit_retries, keep real_retries same
        raise task.retry(
            countdown=int(exc.wait_time) + 1,
            args=[task.request.args[0], real_retries, rate_limit_retries + 1],  # Only rate counter increases
            exc=exc
        )
        
    except ConnectionError as exc:
        # THIS IS A REAL ERROR - will increment real_retries
        if real_retries >= MAX_REAL_RETRIES:
            logger.error(f"Max real retries exceeded for {country_name}")
            return
            
        logger.error(f"Connection error for {country_name} (real retry #{real_retries + 1})")
        
        # Increment ONLY real_retries, keep rate_limit_retries same
        raise task.retry(
            args=[task.request.args[0], real_retries + 1, rate_limit_retries],  # Only real counter increases
            exc=exc
        )
        
    except ValidationError as exc:
        # THIS IS A REAL ERROR - will increment real_retries
        logger.error(f"Validation error for {country_name}: {exc}")
        if real_retries >= MAX_REAL_RETRIES:
            return
            
        raise task.retry(
            args=[task.request.args[0], real_retries + 1, rate_limit_retries],  # Only real counter increases
            exc=exc
        )
        
    except Exception as exc:
        # ANY OTHER ERROR IS A REAL ERROR - will increment real_retries
        if real_retries >= MAX_REAL_RETRIES:
            logger.error(f"Max real retries exceeded for {country_name}")
            return
            
        logger.error(f"Unknown error for {country_name} (real retry #{real_retries + 1}): {exc}")
        
        # Increment ONLY real_retries, keep rate_limit_retries same
        raise task.retry(
            args=[task.request.args[0], real_retries + 1, rate_limit_retries],  # Only real counter increases
            exc=exc
        )









