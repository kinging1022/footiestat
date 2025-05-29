from celery import shared_task
from django.core.cache import cache
from .models import Team, Country , League , LeagueTableSnapshot
from celery.exceptions import Retry
from django.db import transaction
import logging
import requests

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




@shared_task(bind=True, max_retries=3, default_retry_delay=5)
def populate_standings_task(self, league_ids):
    """
    Celery task to populate standings for multiple leagues.
    
    Args:
        league_ids (list): IDs of leagues to fetch standings for.
    """
    try:
        leagues = League.objects.filter(id__in=league_ids).in_bulk(field_name='id')
        
        for league_id in league_ids:
            league = leagues.get(league_id)
            if not league:
                logger.warning(f"League {league_id} not found in database")
                continue
            _process_single_league(self, league_id, league)
                
    except Exception as exc:
        logger.error(f"Failed to process batch: {exc}", exc_info=True)
        self.retry(exc=exc, countdown=5 * (2 ** self.request.retries))  # Exponential backoff


def _process_single_league(task, league_id, league):
    from football.api_client import get_league_table  # Move to top if possible
    
    try:
        if task.request.retries >= task.max_retries:
            logger.error(f"Max retries exceeded for league {league_id}")
            return
            
        response = get_league_table(league_id=league_id, season=league.season)
        
        # Validate API response
        if not response or response.get('results') == 0 or not isinstance(response.get('response'), list):
            logger.error(f"Invalid API response for league {league_id}")
            task.retry()
            return
                
        # Validate standings structure
        try:
            standings = response["response"][0]["league"]["standings"][0]
        except (KeyError, IndexError) as e:
            logger.error(f"Malformed standings data for league {league_id}: {e}")
            return  # Don't retry for data errors
                
        # Get all team IDs from standings first
        team_ids = [t['team']['id'] for t in standings]
        logger.info(f"Found {len(team_ids)} teams in standings for league {league_id}")
        
        # Process teams - Create teams_map with proper error handling
        teams = Team.objects.filter(id__in=team_ids)
        teams_map = {team.id: team for team in teams}
        
        # Check for missing teams
        missing_team_ids = set(team_ids) - teams_map.keys()
        if missing_team_ids:
            logger.warning(f"Missing teams in database for league {league_id}: {missing_team_ids}")
        
        table_list = []
        
        for team_data in standings:
            team_id = team_data['team']['id']
            team = teams_map.get(team_id)
            
            # Skip if team doesn't exist in database
            if not team:
                logger.warning(f"Skipping team {team_id} - not found in database")
                continue
                
            # Create LeagueTableSnapshot with proper error handling
            try:
                snapshot = LeagueTableSnapshot(
                    league=league,
                    season=league.season,
                    rank=team_data["rank"],
                    team=team, 
                    points=team_data["points"],
                    goals_for=team_data.get('all', {}).get('goals', {}).get('for', 0),
                    goals_against=team_data.get('all', {}).get('goals', {}).get('against', 0),
                    goal_difference=team_data["goalsDiff"],
                    matches_played=team_data.get("all", {}).get("played", 0),
                    wins=team_data.get("all", {}).get("win", 0),
                    draws=team_data.get("all", {}).get("draw", 0),
                    losses=team_data.get("all", {}).get("lose", 0),
                    last_five=team_data.get("form", ""),  
                    home_stat=team_data.get("home", {}),
                    away_stat=team_data.get("away", {})
                )
                table_list.append(snapshot)
            except Exception as e:
                logger.error(f"Error creating snapshot for team {team_id} in league {league_id}: {e}")
                continue
        
        # Bulk create with proper transaction and error handling
        if table_list:
            try:
                with transaction.atomic():
                    # Clear existing snapshots for this league/season to avoid duplicates
                    existing_count = LeagueTableSnapshot.objects.filter(
                        league=league, 
                        season=league.season
                    ).count()
                    
                    if existing_count > 0:
                        logger.info(f"Deleting {existing_count} existing snapshots for league {league_id}")
                        LeagueTableSnapshot.objects.filter(
                            league=league, 
                            season=league.season
                        ).delete()
                    
                    # Create new snapshots
                    created_snapshots = LeagueTableSnapshot.objects.bulk_create(
                        table_list, 
                        ignore_conflicts=True 
                    )
                    
                    # Verify creation
                    actual_count = LeagueTableSnapshot.objects.filter(
                        league=league, 
                        season=league.season
                    ).count()
                    
                    logger.info(f"Successfully created {len(created_snapshots)} snapshots for league {league_id}")
                    logger.info(f"Verified {actual_count} snapshots exist in database for league {league_id}")
                    
            except Exception as e:
                logger.error(f"Database error creating snapshots for league {league_id}: {e}", exc_info=True)
                # Re-raise to trigger retry
                raise
        else:
            logger.warning(f"No valid snapshots to create for league {league_id}")
            
    except requests.RequestException as exc:  # Retry only on network issues
        logger.warning(f"Retrying league {league_id} due to API error: {exc}")
        task.retry(exc=exc)
    except Exception as exc:
        logger.error(f"Unexpected error processing league {league_id}: {exc}", exc_info=True)
        # Don't retry on unexpected errors to avoid infinite loops
        return