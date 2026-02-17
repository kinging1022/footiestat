from django.db import transaction
from django.core.cache import cache

import logging
from football.models import Fixture, TeamFormSnapshot,FixtureIngestion, League, Team, HeadToHeadMatch, LeagueTableSnapshot, FixtureAdvancedStats
from football.utils import format_date_time


logger = logging.getLogger(__name__)

# Configuration constants
BATCH_SIZE = 50
CACHE_TIMEOUT_SUCCESS = 3600 * 24  # 24 hours
CACHE_TIMEOUT_NO_DATA = 3600 * 6   # 6 hours for empty results
BULK_CREATE_BATCH_SIZE = 200
MAX_RETRIES = 3


logger = logging.getLogger(__name__)

BULK_CREATE_BATCH_SIZE = 200



logger = logging.getLogger(__name__)

BULK_CREATE_BATCH_SIZE = 200


def fetch_and_process_day(date_str: str) -> dict:
    """
    Fetch and process fixtures for a single day.
    Used by both bootstrap command and daily Celery task.
    
    Args:
        date_str: Date in 'YYYY-MM-DD' format
        
    Returns:
        dict: Summary with created, updated, skipped counts
    """
    logger.info(f"📅 Fetching fixtures for {date_str}")
    
    # Fetch from API
    from football.api_client import get_fixtures
    fixtures_data = get_fixtures(date=date_str)
    
    if not fixtures_data or 'response' not in fixtures_data:
        logger.warning(f"⚠️ No data returned for {date_str}")
        return {
            'status': 'no_data',
            'date': date_str,
            'created': 0,
            'updated': 0,
            'skipped': 0,
            'fixture_ids': []
        }
    
    if not fixtures_data['response']:
        logger.info(f"✓ No fixtures scheduled for {date_str}")
        return {
            'status': 'success',
            'date': date_str,
            'created': 0,
            'updated': 0,
            'skipped': 0,
            'fixture_ids': []
        }
    
    # Process the fixtures
    with transaction.atomic():
        # Extract IDs
        league_ids = {f['league']['id'] for f in fixtures_data['response'] 
                     if 'league' in f and 'id' in f['league']}
        team_ids = set()
        
        for f in fixtures_data['response']:
            if 'teams' in f:
                if 'home' in f['teams'] and 'id' in f['teams']['home']:
                    team_ids.add(f['teams']['home']['id'])
                if 'away' in f['teams'] and 'id' in f['teams']['away']:
                    team_ids.add(f['teams']['away']['id'])
        
        # Fetch existing records
        existing_leagues = {l.id: l for l in League.objects.filter(id__in=league_ids)}
        existing_teams = {t.id: t for t in Team.objects.filter(id__in=team_ids)}
        
        # Track missing data
        missing_leagues = league_ids - existing_leagues.keys()
        missing_teams = team_ids - existing_teams.keys()
        
        if missing_leagues:
            logger.warning(f"Missing leagues for {date_str}: {missing_leagues}")
        if missing_teams:
            logger.warning(f"Missing teams for {date_str}: {missing_teams}")
        
        fixtures_to_create = []
        fixtures_to_update = []
        skipped_fixtures = []
        
        # Get existing fixture IDs
        existing_fixture_ids = set(
            Fixture.objects.filter(
                id__in=[f['fixture']['id'] for f in fixtures_data['response']]
            ).values_list('id', flat=True)
        )
        
        for f in fixtures_data['response']:
            try:
                fixture_id = f['fixture']['id']
                league_id = f['league']['id']
                home_team_id = f['teams']['home']['id']
                away_team_id = f['teams']['away']['id']
                
                # Skip if missing required data
                if not (league_id in existing_leagues and 
                       home_team_id in existing_teams and 
                       away_team_id in existing_teams):
                    skipped_fixtures.append(fixture_id)
                    continue
                
                fixture_obj = Fixture(
                    id=fixture_id,
                    date=f['fixture']['date'],
                    league=existing_leagues[league_id],
                    home_team=existing_teams[home_team_id],
                    away_team=existing_teams[away_team_id],
                    referee=f['fixture'].get('referee'),
                    venue=f.get('fixture', {}).get('venue', {}).get('name'),
                    status=f['fixture']['status']['short'],
                )
                
                if fixture_id in existing_fixture_ids:
                    fixtures_to_update.append(fixture_obj)
                else:
                    fixtures_to_create.append(fixture_obj)
                    
            except (KeyError, TypeError) as e:
                logger.warning(f"Skipping malformed fixture: {e}")
                skipped_fixtures.append(f.get('fixture', {}).get('id', 'unknown'))
                continue
        
        # Create new fixtures
        created_count = 0
        if fixtures_to_create:
            created_fixtures = Fixture.objects.bulk_create(
                fixtures_to_create,
                batch_size=BULK_CREATE_BATCH_SIZE,
                ignore_conflicts=True
            )
            created_count = len(created_fixtures)
            
            # Create FixtureIngestion records
            ingestions = [
                FixtureIngestion(
                    fixture_id=f.id,
                    needs_h2h=True,
                    needs_form=True,
                    needs_standings=True,
                    needs_advanced_stats=True,
                    is_fully_processed=False
                )
                for f in fixtures_to_create
            ]
            FixtureIngestion.objects.bulk_create(
                ingestions,
                batch_size=BULK_CREATE_BATCH_SIZE,
                ignore_conflicts=True
            )
        
        # Update existing fixtures
        updated_count = 0
        if fixtures_to_update:
            for fixture in fixtures_to_update:
                Fixture.objects.filter(id=fixture.id).update(
                    status=fixture.status,
                    referee=fixture.referee,
                    venue=fixture.venue
                )
            updated_count = len(fixtures_to_update)
            
            # Reset processing flags if updated
            FixtureIngestion.objects.filter(
                fixture_id__in=[f.id for f in fixtures_to_update]
            ).update(
                needs_h2h=True,
                needs_form=True,
                needs_standings=True,
                needs_advanced_stats=True,
                is_fully_processed=False,
                h2h_retry_count=0,
                form_retry_count=0,
                standings_retry_count=0,
                advanced_stats_retry_count=0,
                fully_processed_at=None
            )
        
        all_fixture_ids = [f.id for f in (fixtures_to_create + fixtures_to_update)]
        
        logger.info(f"✅ {date_str}: {created_count} created, {updated_count} updated, {len(skipped_fixtures)} skipped")
        
        return {
            'status': 'success',
            'date': date_str,
            'created': created_count,
            'updated': updated_count,
            'skipped': len(skipped_fixtures),
            'fixture_ids': all_fixture_ids
        }



def process_single_h2h(fixture_id: int, home_team_id: int, away_team_id: int) -> dict:
    """
    Process head-to-head data for a single fixture.
    
    Args:
        fixture_id: ID of the fixture
        home_team_id: ID of home team
        away_team_id: ID of away team
    
    Returns:
        dict: Processing result with status and details
    """
    # Input validation
    if not isinstance(fixture_id, int) or fixture_id <= 0:
        raise ValueError(f"Invalid fixture_id: {fixture_id}")
    if not isinstance(home_team_id, int) or home_team_id <= 0:
        raise ValueError(f"Invalid home_team_id: {home_team_id}")
    if not isinstance(away_team_id, int) or away_team_id <= 0:
        raise ValueError(f"Invalid away_team_id: {away_team_id}")
    
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
        
        logger.info(f"No H2H data found for fixture {fixture_id}")
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
            HeadToHeadMatch.objects.bulk_create(
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


def process_single_team_form(team_id: int, season: int) -> dict:
    """
    Process and save team previous form.
    
    Args:
        team_id: ID of the team
        season: Season year
    
    Returns:
        dict: Processing result with status and details
    """
    # Input validation
    if not isinstance(team_id, int) or team_id <= 0:
        raise ValueError(f"Invalid team_id: {team_id}")
    if not isinstance(season, int) or season < 1900 or season > 2100:
        raise ValueError(f"Invalid season: {season}")
    
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
            TeamFormSnapshot.objects.bulk_create(
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


def process_single_league_standings(league_id: int, season: int) -> dict:
    """
    Update standings for a specific league and season.
    
    Args:
        league_id: ID of the league
        season: Season year
    
    Returns:
        dict: Processing result with status and details
    """
    # Input validation
    if not isinstance(league_id, int) or league_id <= 0:
        raise ValueError(f"Invalid league_id: {league_id}")
    
    if not isinstance(season, int) or season < 1900 or season > 2100:
        raise ValueError(f"Invalid season: {season}")
    
    # Check cache first
    cache_key = f"standings_{league_id}_{season}"
    if cache.get(cache_key):
        logger.debug(f"Standings already cached for league {league_id} season {season}")
        return {"status": "cached", "league_id": league_id, "season": season}
    
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





def compute_advanced_fixture_stats(fixture):
    """
    Compute and store advanced stats for a fixture.
    
    For home team: Last 5 home matches
    For away team: Last 5 away matches
    Both teams: Last 5 matches vs teams in similar standings position
    """
    
    logger.info(f"Computing advanced stats for fixture {fixture.id}")
    
    # Get team standings to determine "similar rank" range
    try:
        home_standing = LeagueTableSnapshot.objects.get(
            league=fixture.league,
            season=fixture.league.season,
            team=fixture.home_team
        )
        home_rank = home_standing.rank
    except LeagueTableSnapshot.DoesNotExist:
        logger.warning(f"No standings found for home team {fixture.home_team.id}")
        home_rank = None
    
    try:
        away_standing = LeagueTableSnapshot.objects.get(
            league=fixture.league,
            season=fixture.league.season,
            team=fixture.away_team
        )
        away_rank = away_standing.rank
    except LeagueTableSnapshot.DoesNotExist:
        logger.warning(f"No standings found for away team {fixture.away_team.id}")
        away_rank = None
    
    # === HOME TEAM STATS ===
    
    # Last 5 matches overall
    home_last_5 = TeamFormSnapshot.objects.filter(
        team=fixture.home_team,
        season=fixture.league.season,
        match_date__lt=fixture.date  # Only past matches
    ).order_by('-match_date')[:5]
    
    # Last 5 HOME matches (using is_home flag) ✅
    home_last_5_home = TeamFormSnapshot.objects.filter(
        team=fixture.home_team,
        season=fixture.league.season,
        is_home=True,  # ✅ Already filtered by the flag!
        match_date__lt=fixture.date
    ).order_by('-match_date')[:5]
    
    # Last 5 vs teams in similar standings (within ±3 positions of away team's rank)
    home_vs_similar = []
    if away_rank:
        similar_rank_min = max(1, away_rank - 3)
        similar_rank_max = away_rank + 3
        
        # Get team IDs in similar rank range
        similar_teams = LeagueTableSnapshot.objects.filter(
            league=fixture.league,
            season=fixture.league.season,
            rank__gte=similar_rank_min,
            rank__lte=similar_rank_max
        ).values_list('team_id', flat=True)
        
        home_vs_similar = TeamFormSnapshot.objects.filter(
            team=fixture.home_team,
            season=fixture.league.season,
            opponent_id__in=similar_teams,
            match_date__lt=fixture.date
        ).order_by('-match_date')[:5]
    
    # === AWAY TEAM STATS ===
    
    # Last 5 matches overall
    away_last_5 = TeamFormSnapshot.objects.filter(
        team=fixture.away_team,
        season=fixture.league.season,
        match_date__lt=fixture.date
    ).order_by('-match_date')[:5]
    
    # Last 5 AWAY matches (using is_home=False flag) ✅
    away_last_5_away = TeamFormSnapshot.objects.filter(
        team=fixture.away_team,
        season=fixture.league.season,
        is_home=False,  # ✅ Already filtered by the flag!
        match_date__lt=fixture.date
    ).order_by('-match_date')[:5]
    
    # Last 5 vs teams in similar standings (within ±3 positions of home team's rank)
    away_vs_similar = []
    if home_rank:
        similar_rank_min = max(1, home_rank - 3)
        similar_rank_max = home_rank + 3
        
        similar_teams = LeagueTableSnapshot.objects.filter(
            league=fixture.league,
            season=fixture.league.season,
            rank__gte=similar_rank_min,
            rank__lte=similar_rank_max
        ).values_list('team_id', flat=True)
        
        away_vs_similar = TeamFormSnapshot.objects.filter(
            team=fixture.away_team,
            season=fixture.league.season,
            opponent_id__in=similar_teams,
            match_date__lt=fixture.date
        ).order_by('-match_date')[:5]
    
    # === HELPER FUNCTIONS ===
    
    def serialize_form(snapshots):
        """Convert QuerySet to JSON-serializable list"""
        return [
            {
                'fixture_id': snap.fixture_id,
                'opponent': snap.opponent.name,
                'opponent_id': snap.opponent.id,
                'result': snap.result,  # W/D/L
                'home_goals': snap.home_fulltime_goals,
                'away_goals': snap.away_fulltime_goals,
                'is_home': snap.is_home,
                'date': snap.match_date.isoformat(),
                'league': snap.league_name,
            }
            for snap in snapshots
        ]
    
    def calculate_stats(snapshots):
        """Calculate W/D/L and goals from snapshots"""
        wins = sum(1 for s in snapshots if s.result == 'W')
        draws = sum(1 for s in snapshots if s.result == 'D')
        losses = sum(1 for s in snapshots if s.result == 'L')
        
        goals_scored = 0
        goals_conceded = 0
        
        for s in snapshots:
            if s.home_fulltime_goals is not None and s.away_fulltime_goals is not None:
                if s.is_home:
                    goals_scored += s.home_fulltime_goals
                    goals_conceded += s.away_fulltime_goals
                else:
                    goals_scored += s.away_fulltime_goals
                    goals_conceded += s.home_fulltime_goals
        
        return {
            'wins': wins,
            'draws': draws,
            'losses': losses,
            'goals_scored': goals_scored,
            'goals_conceded': goals_conceded
        }
    
    # Calculate aggregated stats
    home_overall_stats = calculate_stats(home_last_5)
    home_home_stats = calculate_stats(home_last_5_home)
    away_overall_stats = calculate_stats(away_last_5)
    away_away_stats = calculate_stats(away_last_5_away)
    
    # === CREATE OR UPDATE ADVANCED STATS ===
    
    advanced_stats, created = FixtureAdvancedStats.objects.update_or_create(
        fixture=fixture,
        defaults={
            # Home team - overall form
            'home_last_5_form': serialize_form(home_last_5),
            'home_wins_last_5': home_overall_stats['wins'],
            'home_draws_last_5': home_overall_stats['draws'],
            'home_losses_last_5': home_overall_stats['losses'],
            'home_goals_scored_last_5': home_overall_stats['goals_scored'],
            'home_goals_conceded_last_5': home_overall_stats['goals_conceded'],
            
            # Home team - home form (is_home=True)
            'home_last_5_home_form': serialize_form(home_last_5_home),
            'home_home_wins_last_5': home_home_stats['wins'],
            'home_home_draws_last_5': home_home_stats['draws'],
            'home_home_losses_last_5': home_home_stats['losses'],
            
            # Home team - vs similar ranked opponents
            'home_last_5_vs_similar_rank': serialize_form(home_vs_similar),
            
            # Away team - overall form
            'away_last_5_form': serialize_form(away_last_5),
            'away_wins_last_5': away_overall_stats['wins'],
            'away_draws_last_5': away_overall_stats['draws'],
            'away_losses_last_5': away_overall_stats['losses'],
            'away_goals_scored_last_5': away_overall_stats['goals_scored'],
            'away_goals_conceded_last_5': away_overall_stats['goals_conceded'],
            
            # Away team - away form (is_home=False)
            'away_last_5_away_form': serialize_form(away_last_5_away),
            'away_away_wins_last_5': away_away_stats['wins'],
            'away_away_draws_last_5': away_away_stats['draws'],
            'away_away_losses_last_5': away_away_stats['losses'],
            
            # Away team - vs similar ranked opponents
            'away_last_5_vs_similar_rank': serialize_form(away_vs_similar),
        }
    )
    
    action = 'Created' if created else 'Updated'
    logger.info(f"✅ {action} advanced stats for fixture {fixture.id}")
    
    return advanced_stats