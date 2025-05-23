from celery import shared_task
from django.core.cache import cache
from .models import Team, Country
from celery.exceptions import Retry
import logging

logger = logging.getLogger(__name__)

@shared_task
def test_task():
    """Simple test task for Celery verification"""
    print("This is a test task.")

@shared_task(bind=True, max_retries=3, default_retry_delay=5)
def populate_teams_task(self, country_names):
    """
    Celery task to process teams for a batch of countries
    
    Args:
        countries_batch: Dictionary mapping country names to Country objects
    """
    try:

        countries = Country.objects.filter(name__in=country_names).in_bulk(field_name='name')
        
        for country_name in country_names:
            country = countries.get(country_name)
            if not country: 
                logger.warning(f"Country {country_name} not found in database")
                continue
            _process_single_country(self, country_name, country)
    except Exception as exc:
        logger.error(f"Failed to process batch: {exc}")
 
        self.retry(exc=exc)







def _process_single_country(task, country_name, country):
    """
    Process a single country to fetch and create teams.
    
    Args:
        task: Task instance (for retry functionality)
        country_name: Name of the country
        country: Country model instance
    """
    try:
        # Import here to avoid circular imports
        if task.request.retries >= task.max_retries:
            logger.error(f"Max retries exceeded for {country_name}")
            return
        
        from football.api_client import get_team_details
        response = get_team_details(country=country_name)
        if not response or not isinstance(response.get('response'), list):
            logger.error(f"Invalid API response for {country_name}")
            task.retry()
            return
            
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
            Team.objects.bulk_create(
                teams,
                batch_size=500,
                ignore_conflicts=True
            )
            
    except Exception as exc:
        logger.error(f"Failed to process {country_name}: {exc}")
        task.retry(exc=exc)