from celery import shared_task, group, chain
from django.db import transaction
from django.core.cache import cache
from datetime import datetime, timedelta
import logging

from football.api_client import RateLimitExceeded
from football.models import Fixture, TeamFormSnapshot, League, Team, HeadToHeadMatch, LeagueTableSnapshot
from football.utils import get_fixtures_for_processing, format_date_time


logger = logging.getLogger(__name__)

# Configuration constants
BATCH_SIZE = 50
CACHE_TIMEOUT_SUCCESS = 3600 * 24  # 24 hours
CACHE_TIMEOUT_NO_DATA = 3600 * 6   # 6 hours for empty results
BULK_CREATE_BATCH_SIZE = 200
MAX_RETRIES = 3


@shared_task(bind=True, max_retries=MAX_RETRIES, default_retry_delay=30)
def fetch_fixtures_batch(self, start_date: str, end_date: str):
    """
    Fetch fixtures for a given date range and process them in batches.
    
    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
    
    Returns:
        list: List of created fixture IDs
    
    Raises:
        Exception: If fixture fetching fails after retries
    """
    # Input validation
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got start_date={start_date}, end_date={end_date}")
    
    try:
        from football.api_client import get_fixtures
        fixtures_data = get_fixtures(from_date=start_date, to_date=end_date)
        
        # Validate API response
        if not fixtures_data or 'response' not in fixtures_data:
            logger.error(f"Invalid API response structure for dates {start_date} to {end_date}")
            raise ValueError("Invalid API response: missing 'response' key")
        
        logger.info(f"Fetched {len(fixtures_data['response'])} fixtures for date {start_date} to {end_date}")
        
        with transaction.atomic():
            # Extract league and team IDs from fixtures data
            league_ids = {f['league']['id'] for f in fixtures_data['response'] if 'league' in f and 'id' in f['league']}
            team_ids = set()
            
            for f in fixtures_data['response']:
                if 'teams' in f:
                    if 'home' in f['teams'] and 'id' in f['teams']['home']:
                        team_ids.add(f['teams']['home']['id'])
                    if 'away' in f['teams'] and 'id' in f['teams']['away']:
                        team_ids.add(f['teams']['away']['id'])
            
            # Fetch existing teams and leagues from the database
            existing_leagues = {l.id: l for l in League.objects.filter(id__in=league_ids)}
            existing_teams = {t.id: t for t in Team.objects.filter(id__in=team_ids)}
            
            # Track missing data
            missing_leagues = league_ids - existing_leagues.keys()
            missing_teams = team_ids - existing_teams.keys()
            
            if missing_leagues:
                logger.warning(f"Missing leagues in database: {missing_leagues}")
            if missing_teams:
                logger.warning(f"Missing teams in database: {missing_teams}")
            
            fixtures_to_create = []
            skipped_fixtures = []
            
            for f in fixtures_data['response']:
                try:
                    fixture_id = f['fixture']['id']
                    league_id = f['league']['id']
                    home_team_id = f['teams']['home']['id']
                    away_team_id = f['teams']['away']['id']
                    
                    if (league_id in existing_leagues and 
                        home_team_id in existing_teams and 
                        away_team_id in existing_teams):
                        
                        fixtures_to_create.append(
                            Fixture(
                                id=fixture_id,
                                date=f['fixture']['date'],
                                league=existing_leagues[league_id],
                                home_team=existing_teams[home_team_id],
                                away_team=existing_teams[away_team_id],
                                referee=f['fixture'].get('referee'),
                                venue=f.get('fixture', {}).get('venue', {}).get('name'),
                                status=f['fixture']['status']['short'],
                            )
                        )
                    else:
                        skipped_fixtures.append(fixture_id)
                        
                except (KeyError, TypeError) as e:
                    logger.warning(f"Skipping malformed fixture data: {e}")
                    continue
            
            if skipped_fixtures:
                logger.warning(f"Skipped {len(skipped_fixtures)} fixtures due to missing league/team data: {skipped_fixtures[:10]}...")
            
            if fixtures_to_create:
                # Use update_or_create behavior by checking existing first
                existing_fixture_ids = set(
                    Fixture.objects.filter(
                        id__in=[f.id for f in fixtures_to_create]
                    ).values_list('id', flat=True)
                )
                
                # Only create truly new fixtures
                new_fixtures = [f for f in fixtures_to_create if f.id not in existing_fixture_ids]
                
                if new_fixtures:
                    created_fixtures = Fixture.objects.bulk_create(
                        new_fixtures,
                        batch_size=BULK_CREATE_BATCH_SIZE,
                        ignore_conflicts=True
                    )
                    logger.info(f"Created {len(created_fixtures)} new fixtures, {len(existing_fixture_ids)} already existed")
                    
                    # Return all fixture IDs (new + existing)
                    all_fixture_ids = [f.id for f in fixtures_to_create]
                    return all_fixture_ids
                else:
                    logger.info(f"All {len(fixtures_to_create)} fixtures already exist")
                    return [f.id for f in fixtures_to_create]
        
        return []
    
    except RateLimitExceeded as exc:
        logger.warning(f"Rate limit hit fetching fixtures, retrying in {exc.wait_time}s")
        raise self.retry(countdown=exc.wait_time, exc=exc)
    
    except Exception as exc:
        countdown = 30 * (2 ** self.request.retries)
        logger.error(f"Error fetching fixtures (retry {self.request.retries + 1}/{self.max_retries}): {exc}")
        raise self.retry(countdown=countdown, exc=exc)


@shared_task(bind=True, max_retries=MAX_RETRIES, default_retry_delay=30)
def process_fixtures_h2h(self, fixture_id: int, home_team_id: int, away_team_id: int):
    """
    Process head-to-head data for a fixture.
    
    Args:
        fixture_id: ID of the fixture
        home_team_id: ID of home team
        away_team_id: ID of away team
    
    Returns:
        dict: Processing result with status and details
    
    Raises:
        Exception: If processing fails after retries
    """
    # Input validation
    if not isinstance(fixture_id, int) or fixture_id <= 0:
        raise ValueError(f"Invalid fixture_id: {fixture_id}")
    if not isinstance(home_team_id, int) or home_team_id <= 0:
        raise ValueError(f"Invalid home_team_id: {home_team_id}")
    if not isinstance(away_team_id, int) or away_team_id <= 0:
        raise ValueError(f"Invalid away_team_id: {away_team_id}")
    
    try:
        cache_key = f"h2h_{fixture_id}_{home_team_id}_{away_team_id}"

        if cache.get(cache_key):
            logger.debug(f"H2H already processed for fixture {fixture_id}")
            return {"status": "cached", "fixture_id": fixture_id}
        
        # Fetch head-to-head data
        from football.api_client import get_fixture_head_to_head
        
        h2h_data = get_fixture_head_to_head(f"{home_team_id}-{away_team_id}")

        # Validate API response
        if (not h2h_data or 
            'response' not in h2h_data or 
            not h2h_data['response'] or 
            len(h2h_data['response']) == 0 or 
            h2h_data.get('results', 0) == 0):
            
            logger.info(f"No H2H data found for fixture {fixture_id} between teams {home_team_id} and {away_team_id}")
            cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
            return {"status": "no_data", "fixture_id": fixture_id}

        with transaction.atomic():
            try:
                fixture = Fixture.objects.get(id=fixture_id)
            except Fixture.DoesNotExist:
                logger.warning(f"Fixture {fixture_id} not found in database")
                raise ValueError(f"Fixture {fixture_id} does not exist")

            h2h_matches = []
            skipped_count = 0
            
            for h in h2h_data['response']:
                try:
                    # Validate required fields
                    if not all([
                        h.get('fixture', {}).get('id'),
                        h.get('fixture', {}).get('date'),
                        h.get('teams', {}).get('home', {}).get('name'),
                        h.get('teams', {}).get('away', {}).get('name'),
                    ]):
                        skipped_count += 1
                        continue
                    
                    h2h_matches.append(
                        HeadToHeadMatch(
                            fixture=fixture,
                            past_fixture_id=h['fixture']['id'],
                            date=format_date_time(h['fixture']['date']),
                            home_name=h['teams']['home']['name'],
                            away_name=h['teams']['away']['name'],
                            league_name=h.get('league', {}).get('name', 'Unknown'),
                            home_fulltime_goals=h.get('score', {}).get('fulltime', {}).get('home'),
                            away_fulltime_goals=h.get('score', {}).get('fulltime', {}).get('away'),
                            home_half_time_goals=h.get('score', {}).get('halftime', {}).get('home'),
                            away_half_time_goals=h.get('score', {}).get('halftime', {}).get('away'),
                            home_extra_time_goals=h.get('score', {}).get('extratime', {}).get('home') if h.get('score', {}).get('extratime') else None,
                            away_extra_time_goals=h.get('score', {}).get('extratime', {}).get('away') if h.get('score', {}).get('extratime') else None,
                            home_penalty_goals=h.get('score', {}).get('penalty', {}).get('home') if h.get('score', {}).get('penalty') else None,
                            away_penalty_goals=h.get('score', {}).get('penalty', {}).get('away') if h.get('score', {}).get('penalty') else None,
                        )
                    )
                except (KeyError, TypeError) as e:
                    logger.warning(f"Skipping malformed H2H match data for fixture {fixture_id}: {e}")
                    skipped_count += 1
                    continue

            if h2h_matches:
                created = HeadToHeadMatch.objects.bulk_create(
                    h2h_matches,
                    batch_size=BULK_CREATE_BATCH_SIZE,
                    ignore_conflicts=True
                )
                logger.info(f"Processed H2H for fixture {fixture_id}: {len(h2h_matches)} matches, {skipped_count} skipped")
                
                cache.set(cache_key, True, timeout=CACHE_TIMEOUT_SUCCESS)
                return {
                    "status": "success",
                    "fixture_id": fixture_id,
                    "matches_processed": len(h2h_matches),
                    "skipped": skipped_count
                }
            else:
                logger.warning(f"No valid H2H data for fixture {fixture_id}, {skipped_count} records skipped")
                cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
                return {
                    "status": "no_valid_data",
                    "fixture_id": fixture_id,
                    "skipped": skipped_count
                }
            
    except RateLimitExceeded as exc:
        logger.warning(f"Rate limit hit for fixture {fixture_id}, retrying in {exc.wait_time}s")
        raise self.retry(countdown=exc.wait_time, exc=exc)
    
    except ValueError as exc:
        # Don't retry validation errors
        logger.error(f"Validation error for fixture {fixture_id}: {exc}")
        raise
    
    except Exception as exc:
        countdown = 30 * (2 ** self.request.retries)
        logger.error(f"Error processing H2H for fixture {fixture_id} (retry {self.request.retries + 1}/{self.max_retries}): {exc}")
        raise self.retry(countdown=countdown, exc=exc)


@shared_task(bind=True, max_retries=MAX_RETRIES, default_retry_delay=30)
def process_team_form(self, team_id: int, season: int):
    """
    Process and save team previous form.
    
    Args:
        team_id: ID of the team
        season: Season year
    
    Returns:
        dict: Processing result with status and details
    
    Raises:
        Exception: If processing fails after retries
    """
    # Input validation
    if not isinstance(team_id, int) or team_id <= 0:
        raise ValueError(f"Invalid team_id: {team_id}")
    if not isinstance(season, int) or season < 1900 or season > 2100:
        raise ValueError(f"Invalid season: {season}")
    
    try:
        cache_key = f"team_form_{team_id}_{season}"
        if cache.get(cache_key):
            logger.debug(f"Team form already processed for team {team_id} season {season}")
            return {"status": "cached", "team_id": team_id, "season": season}
        
        try:
            team = Team.objects.get(id=team_id)
        except Team.DoesNotExist:
            logger.warning(f"Team {team_id} not found in database")
            raise ValueError(f"Team {team_id} does not exist")

        from football.api_client import get_fixtures
        previous_fixtures = get_fixtures(team_id=team_id, season=season, status='FT-AET-PEN')

        # Validate API response
        if (not previous_fixtures or 
            'response' not in previous_fixtures or 
            not previous_fixtures['response'] or 
            len(previous_fixtures['response']) == 0):
            
            logger.info(f"No previous fixtures found for team {team_id} in season {season}")
            cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
            return {"status": "no_data", "team_id": team_id, "season": season}
        
        fixtures = previous_fixtures['response']

        # Get opponent IDs
        opponent_ids = set()
        for fixture in fixtures:
            try:
                is_home = fixture.get('teams', {}).get('home', {}).get('id') == team_id
                opponent_id = (fixture.get('teams', {}).get('away', {}).get('id') if is_home 
                              else fixture.get('teams', {}).get('home', {}).get('id'))
                if opponent_id:
                    opponent_ids.add(opponent_id)
            except (KeyError, TypeError, AttributeError):
                continue

        opponents = {t.id: t for t in Team.objects.filter(id__in=opponent_ids)}
        
        # Track missing opponents
        missing_opponents = opponent_ids - opponents.keys()
        if missing_opponents:
            logger.warning(f"Missing opponent teams for team {team_id}: {missing_opponents}")

        with transaction.atomic():
            form_snapshots = []
            skipped_count = 0
            
            for fixture in fixtures:
                try:
                    is_home = fixture.get('teams', {}).get('home', {}).get('id') == team_id
                    home_goals = fixture.get('goals', {}).get('home')
                    away_goals = fixture.get('goals', {}).get('away')
                    
                    # Skip if essential data is missing
                    if home_goals is None or away_goals is None:
                        skipped_count += 1
                        continue
                    
                    # Calculate result from team's perspective
                    if is_home:
                        result = 'W' if home_goals > away_goals else ('L' if home_goals < away_goals else 'D')
                    else:
                        result = 'W' if away_goals > home_goals else ('L' if away_goals < home_goals else 'D')

                    # Get opponent team object
                    opponent_id = (fixture.get('teams', {}).get('away', {}).get('id') if is_home 
                                  else fixture.get('teams', {}).get('home', {}).get('id'))
                    opponent_team = opponents.get(opponent_id)
                    
                    if not opponent_team:
                        logger.warning(f"Opponent team {opponent_id} not found for fixture {fixture.get('fixture', {}).get('id')}")
                        skipped_count += 1
                        continue
                    
                    form_snapshots.append(
                        TeamFormSnapshot(
                            team=team,
                            league_name=fixture.get('league', {}).get('name'),
                            league_id=fixture.get('league', {}).get('id'),
                            season=season,
                            fixture_id=fixture.get('fixture', {}).get('id'),
                            match_date=format_date_time(fixture.get('fixture', {}).get('date')),
                            is_home=is_home,
                            opponent=opponent_team,
                            home_fulltime_goals=fixture.get('score', {}).get('fulltime', {}).get('home'),
                            away_fulltime_goals=fixture.get('score', {}).get('fulltime', {}).get('away'),
                            home_half_time_goals=fixture.get('score', {}).get('halftime', {}).get('home'),
                            away_half_time_goals=fixture.get('score', {}).get('halftime', {}).get('away'),
                            home_extra_time_goals=fixture.get('score', {}).get('extratime', {}).get('home') if fixture.get('score', {}).get('extratime') else None,
                            away_extra_time_goals=fixture.get('score', {}).get('extratime', {}).get('away') if fixture.get('score', {}).get('extratime') else None,
                            home_penalty_goals=fixture.get('score', {}).get('penalty', {}).get('home') if fixture.get('score', {}).get('penalty') else None,
                            away_penalty_goals=fixture.get('score', {}).get('penalty', {}).get('away') if fixture.get('score', {}).get('penalty') else None,
                            result=result,
                            round_name=fixture.get('league', {}).get('round')
                        )
                    )
                    
                except (KeyError, TypeError, AttributeError) as e:
                    logger.warning(f"Skipping malformed fixture data for team {team_id}: {e}")
                    skipped_count += 1
                    continue
            
            if form_snapshots:
                created = TeamFormSnapshot.objects.bulk_create(
                    form_snapshots,
                    batch_size=BULK_CREATE_BATCH_SIZE,
                    ignore_conflicts=True
                )
                logger.info(f"Processed team form for team {team_id} season {season}: {len(form_snapshots)} fixtures, {skipped_count} skipped")
                
                cache.set(cache_key, True, timeout=CACHE_TIMEOUT_SUCCESS)
                return {
                    "status": "success",
                    "team_id": team_id,
                    "season": season,
                    "fixtures_processed": len(form_snapshots),
                    "skipped": skipped_count
                }
            else:
                logger.warning(f"No valid form snapshots for team {team_id} season {season}, {skipped_count} skipped")
                cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
                return {
                    "status": "no_valid_data",
                    "team_id": team_id,
                    "season": season,
                    "skipped": skipped_count
                }

    except RateLimitExceeded as exc:
        logger.warning(f"Rate limit hit for team {team_id}, retrying in {exc.wait_time}s")
        raise self.retry(countdown=exc.wait_time, exc=exc)
    
    except ValueError as exc:
        # Don't retry validation errors
        logger.error(f"Validation error for team {team_id}: {exc}")
        raise
    
    except Exception as exc:
        countdown = 30 * (2 ** self.request.retries)
        logger.error(f"Error processing team {team_id} (retry {self.request.retries + 1}/{self.max_retries}): {exc}")
        raise self.retry(countdown=countdown, exc=exc)


@shared_task(bind=True, max_retries=MAX_RETRIES, default_retry_delay=30)
def update_league_standings(self, league_id: int, season: int):
    """
    Update standings for a specific league and season.
    
    Args:
        league_id: ID of the league
        season: Season year
    
    Returns:
        dict: Processing result with status and details
    
    Raises:
        Exception: If processing fails after retries
    """
    # Input validation
    if not isinstance(league_id, int) or league_id <= 0:
        logger.error(f"Invalid league_id: {league_id}")
        raise ValueError(f"Invalid league_id: {league_id}")
    
    if not isinstance(season, int) or season < 1900 or season > 2100:
        logger.error(f"Invalid season: {season}")
        raise ValueError(f"Invalid season: {season}")
    
    # Check cache first
    cache_key = f"standings_{league_id}_{season}"
    if cache.get(cache_key):
        logger.debug(f"Standings already cached for league {league_id} season {season}")
        return {"status": "cached", "league_id": league_id, "season": season}
    
    try:
        # Get league object
        try:
            league = League.objects.get(id=league_id)
        except League.DoesNotExist:
            logger.warning(f"League {league_id} not found in database")
            raise ValueError(f"League {league_id} does not exist")
        
        logger.info(f"Processing league {league_id} for season {season}")

        # Get League table data from API
        from football.api_client import get_league_table
        response = get_league_table(league_id=league_id, season=season)

        # Validate API response
        if (not response or 
            response.get('results', 0) == 0 or 
            not isinstance(response.get('response'), list) or
            len(response.get('response', [])) == 0):
            
            logger.error(f"Invalid or empty API response for league {league_id}")
            cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
            return {"status": "no_data", "league_id": league_id, "season": season}

        # Validate standings structure
        try:
            standings = response["response"][0]["league"]["standings"][0]
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Malformed standings data for league {league_id}: {e}")
            raise ValueError(f"Malformed standings data: {e}")
        
        # Get all team IDs from standings
        team_ids = []
        for t in standings:
            try:
                team_id = t.get('team', {}).get('id')
                if team_id:
                    team_ids.append(team_id)
            except (KeyError, TypeError, AttributeError):
                continue
        
        logger.info(f"Found {len(team_ids)} teams in standings for league {league_id}")

        # Get teams from database
        teams = Team.objects.filter(id__in=team_ids)
        teams_map = {team.id: team for team in teams}

        # Check for missing teams
        missing_team_ids = set(team_ids) - teams_map.keys()
        if missing_team_ids:
            logger.warning(f"Missing teams in database for league {league_id}: {missing_team_ids}")

        table_list = []
        skipped_count = 0

        for team_data in standings:
            try:
                team_id = team_data.get('team', {}).get('id')
                if not team_id:
                    skipped_count += 1
                    continue
                
                team = teams_map.get(team_id)

                # Skip if team doesn't exist in database
                if not team:
                    logger.warning(f"Skipping team {team_id} - not found in database")
                    skipped_count += 1
                    continue
                
                # Validate required fields
                if team_data.get('rank') is None or team_data.get('points') is None:
                    logger.warning(f"Skipping team {team_id} - missing rank or points")
                    skipped_count += 1
                    continue
                
                # Create LeagueTableSnapshot
                snapshot = LeagueTableSnapshot(
                    league=league,
                    season=season,
                    rank=team_data["rank"],
                    team=team,
                    points=team_data["points"],
                    goals_for=team_data.get('all', {}).get('goals', {}).get('for', 0),
                    goals_against=team_data.get('all', {}).get('goals', {}).get('against', 0),
                    goal_difference=team_data.get("goalsDiff", 0),
                    matches_played=team_data.get("all", {}).get("played", 0),
                    wins=team_data.get("all", {}).get("win", 0),
                    draws=team_data.get("all", {}).get("draw", 0),
                    losses=team_data.get("all", {}).get("lose", 0),
                    last_five=team_data.get("form", ""),
                    home_stat={
                        'played': team_data.get('home', {}).get('played', 0),
                        'wins': team_data.get('home', {}).get('win', 0),
                        'draws': team_data.get('home', {}).get('draw', 0),
                        'losses': team_data.get('home', {}).get('lose', 0),
                        'goals_for': team_data.get('home', {}).get('goals', {}).get('for', 0),
                        'goals_against': team_data.get('home', {}).get('goals', {}).get('against', 0)
                    },
                    away_stat={
                        'played': team_data.get('away', {}).get('played', 0),
                        'wins': team_data.get('away', {}).get('win', 0),
                        'draws': team_data.get('away', {}).get('draw', 0),
                        'losses': team_data.get('away', {}).get('lose', 0),
                        'goals_for': team_data.get('away', {}).get('goals', {}).get('for', 0),
                        'goals_against': team_data.get('away', {}).get('goals', {}).get('against', 0)
                    }
                )
                table_list.append(snapshot)
                
            except (KeyError, TypeError, AttributeError) as e:
                logger.error(f"Error creating snapshot for team {team_id} in league {league_id}: {e}")
                skipped_count += 1
                continue

        if table_list:
            try:
                with transaction.atomic():
                    existing_count = LeagueTableSnapshot.objects.filter(
                        league=league,
                        season=season
                    ).count()

                    if existing_count > 0:
                        logger.info(f"Deleting {existing_count} existing snapshots for league {league_id} season {season}")
                        LeagueTableSnapshot.objects.filter(
                            league=league,
                            season=season
                        ).delete()

                    # Create new snapshots
                    created_snapshots = LeagueTableSnapshot.objects.bulk_create(
                        table_list,
                        batch_size=BULK_CREATE_BATCH_SIZE,
                        ignore_conflicts=True
                    )

                    # Verify creation
                    actual_count = LeagueTableSnapshot.objects.filter(
                        league=league,
                        season=season
                    ).count()

                    logger.info(f"Successfully created {len(created_snapshots)} snapshots for league {league_id} season {season}")
                    logger.info(f"Verified {actual_count} snapshots exist in database")

                    # Only set cache after successful database transactions
                    cache.set(cache_key, True, timeout=CACHE_TIMEOUT_SUCCESS)

                    return {
                        "status": "success",
                        "league_id": league_id,
                        "season": season,
                        "teams_processed": actual_count,
                        "skipped": skipped_count
                    }

            except Exception as e:
                logger.error(f"Database error creating snapshots for league {league_id}: {e}", exc_info=True)
                raise
        else:
            logger.warning(f"No valid snapshots to create for league {league_id}, {skipped_count} teams skipped")
            cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
            return {
                "status": "no_valid_data",
                "league_id": league_id,
                "season": season,
                "skipped": skipped_count
            }
            
    except RateLimitExceeded as exc:
        logger.warning(f"Rate limit hit for league {league_id}, retrying in {exc.wait_time}s")
        raise self.retry(countdown=exc.wait_time, exc=exc)
    
    except ValueError as exc:
        # Don't retry validation errors
        logger.error(f"Validation error for league {league_id}: {exc}")
        raise
    
    except Exception as exc:
        countdown = 30 * (2 ** self.request.retries)
        logger.error(f"Error processing league {league_id} (retry {self.request.retries + 1}/{self.max_retries}): {exc}")
        raise self.retry(countdown=countdown, exc=exc)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def process_football_data_pipeline(self):
    """
    Main data pipeline orchestration.
    
    Returns:
        dict: Pipeline execution result with status and task counts
    
    Raises:
        Exception: If pipeline fails after retries
    """
    try:
        logger.info(f"Starting football data pipeline (attempt {self.request.retries + 1})")
        start_date = datetime.now().strftime('%Y-%m-%d')
        end_date = (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d')

        # Fetch fixtures
        fixture_result = fetch_fixtures_batch.apply_async(args=[start_date, end_date])
        fixtures_ids = fixture_result.get(timeout=300)  # 5 min timeout

        if not fixtures_ids:
            logger.info("Pipeline completed: No new fixtures found.")
            return {"status": "no_fixtures", "message": "No fixtures found for date range"}
        
        fixtures_data = get_fixtures_for_processing(fixtures_ids)
        logger.info(f"Found {len(fixtures_data)} fixtures to process")

        # Organize tasks by type
        h2h_tasks = []
        form_tasks = []
        standings_tasks = []
        unique_teams_seasons = set()  # Track unique (team, season) pairs
        unique_leagues = set()

        for fixture in fixtures_data:
            try:
                # H2H tasks
                h2h_tasks.append(
                    process_fixtures_h2h.si(
                        fixture['id'],
                        fixture['home_team_id'],
                        fixture['away_team_id']
                    )
                )
                
                # Form tasks - avoid duplicates
                home_key = (fixture['home_team_id'], fixture['season'])
                away_key = (fixture['away_team_id'], fixture['season'])
                
                if home_key not in unique_teams_seasons:
                    form_tasks.append(
                        process_team_form.si(fixture['home_team_id'], fixture['season'])
                    )
                    unique_teams_seasons.add(home_key)
                
                if away_key not in unique_teams_seasons:
                    form_tasks.append(
                        process_team_form.si(fixture['away_team_id'], fixture['season'])
                    )
                    unique_teams_seasons.add(away_key)
                
                # League standings
                unique_leagues.add((fixture['league_id'], fixture['season']))
                
            except (KeyError, TypeError) as e:
                logger.warning(f"Skipping malformed fixture data in pipeline: {e}")
                continue

        # Add standings tasks
        for league_id, season in unique_leagues:
            standings_tasks.append(update_league_standings.si(league_id, season))

        # Workflow with batching
        workflow_steps = []

        # Add H2H batches
        for i in range(0, len(h2h_tasks), BATCH_SIZE):
            batch = h2h_tasks[i:i + BATCH_SIZE]
            workflow_steps.append(group(*batch))

        # Add form batches
        for i in range(0, len(form_tasks), BATCH_SIZE):
            batch = form_tasks[i:i + BATCH_SIZE]
            workflow_steps.append(group(*batch))

        # Add standings batches
        for i in range(0, len(standings_tasks), BATCH_SIZE):
            batch = standings_tasks[i:i + BATCH_SIZE]
            workflow_steps.append(group(*batch))

        # Execute workflow
        if workflow_steps:
            workflow = chain(*workflow_steps)
            result = workflow.apply_async()

            total_tasks = len(h2h_tasks) + len(form_tasks) + len(standings_tasks)
            logger.info(f"Pipeline started: {total_tasks} tasks in {len(workflow_steps)} batches")
            
            return {
                "status": "started",
                "total_tasks": total_tasks,
                "h2h_tasks": len(h2h_tasks),
                "form_tasks": len(form_tasks),
                "standings_tasks": len(standings_tasks),
                "batches": len(workflow_steps),
                "batch_size": BATCH_SIZE
            }
        else:
            logger.warning("No tasks to execute in pipeline")
            return {"status": "no_tasks", "message": "No valid tasks generated"}

    except Exception as exc:
        logger.error(f"Pipeline error (retry {self.request.retries + 1}/{self.max_retries}): {exc}", exc_info=True)
        raise self.retry(countdown=300, exc=exc)



























