from django.db import transaction
from django.core.cache import cache
from datetime import date as date_type, datetime
from typing import Optional
import logging

from football.models import (
    Fixture, TeamFormSnapshot, FixtureIngestion, League, Team,
    HeadToHeadMatch, LeagueTableSnapshot, FixtureAdvancedStats,
    FixtureStatistics
)
from football.utils import format_date_time

logger = logging.getLogger(__name__)

# Configuration constants
CACHE_TIMEOUT_SUCCESS  = 3600 * 24  # 24 hours
CACHE_TIMEOUT_NO_DATA  = 3600 * 6   # 6 hours
BULK_CREATE_BATCH_SIZE = 200


def fetch_and_process_day(date_str: str) -> dict:
    """
    Fetch and process fixtures for a single day.
    Used by both bootstrap command and daily Celery task.
    """
    logger.info(f"📅 Fetching fixtures for {date_str}")

    from football.api_client import get_fixtures
    fixtures_data = get_fixtures(date=date_str)

    if not fixtures_data or 'response' not in fixtures_data:
        logger.warning(f"⚠️ No data returned for {date_str}")
        return {
            'status':      'no_data',
            'date':        date_str,
            'created':     0,
            'updated':     0,
            'skipped':     0,
            'fixture_ids': []
        }

    if not fixtures_data['response']:
        logger.info(f"✓ No fixtures scheduled for {date_str}")
        return {
            'status':      'success',
            'date':        date_str,
            'created':     0,
            'updated':     0,
            'skipped':     0,
            'fixture_ids': []
        }

    with transaction.atomic():
        league_ids = {
            f['league']['id']
            for f in fixtures_data['response']
            if 'league' in f and 'id' in f['league']
        }
        team_ids = set()
        for f in fixtures_data['response']:
            if 'teams' in f:
                if 'home' in f['teams'] and 'id' in f['teams']['home']:
                    team_ids.add(f['teams']['home']['id'])
                if 'away' in f['teams'] and 'id' in f['teams']['away']:
                    team_ids.add(f['teams']['away']['id'])

        existing_leagues = {
            l.id: l for l in League.objects.filter(id__in=league_ids)
        }
        existing_teams = {
            t.id: t for t in Team.objects.filter(id__in=team_ids)
        }

        missing_leagues = league_ids - existing_leagues.keys()
        missing_teams   = team_ids   - existing_teams.keys()

        if missing_leagues:
            logger.warning(f"Missing leagues for {date_str}: {missing_leagues}")
        if missing_teams:
            logger.warning(f"Missing teams for {date_str}: {missing_teams}")

        fixtures_to_create = []
        fixtures_to_update = []
        skipped_fixtures   = []

        existing_fixture_ids = set(
            Fixture.objects.filter(
                id__in=[f['fixture']['id'] for f in fixtures_data['response']]
            ).values_list('id', flat=True)
        )

        for f in fixtures_data['response']:
            try:
                fixture_id   = f['fixture']['id']
                league_id    = f['league']['id']
                home_team_id = f['teams']['home']['id']
                away_team_id = f['teams']['away']['id']

                if not (
                    league_id    in existing_leagues and
                    home_team_id in existing_teams   and
                    away_team_id in existing_teams
                ):
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
                skipped_fixtures.append(
                    f.get('fixture', {}).get('id', 'unknown')
                )
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

            # ── Explicit on all flags including needs_detailed_stats ───────
            ingestions = [
                FixtureIngestion(
                    fixture_id=f.id,
                    needs_h2h=True,
                    needs_form=True,
                    needs_standings=True,
                    needs_advanced_stats=True,
                    needs_detailed_stats=True,
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

            # ── Reset ALL flags including needs_detailed_stats ─────────────
            FixtureIngestion.objects.filter(
                fixture_id__in=[f.id for f in fixtures_to_update]
            ).update(
                needs_h2h=True,
                needs_form=True,
                needs_standings=True,
                needs_advanced_stats=True,
                needs_detailed_stats=True,
                is_fully_processed=False,
                h2h_retry_count=0,
                form_retry_count=0,
                standings_retry_count=0,
                advanced_stats_retry_count=0,
                detailed_stats_retry_count=0,
                fully_processed_at=None,
                last_error=None
            )

        all_fixture_ids = [
            f.id for f in (fixtures_to_create + fixtures_to_update)
        ]

        logger.info(
            f"✅ {date_str}: {created_count} created, "
            f"{updated_count} updated, {len(skipped_fixtures)} skipped"
        )

        return {
            'status':      'success',
            'date':        date_str,
            'created':     created_count,
            'updated':     updated_count,
            'skipped':     len(skipped_fixtures),
            'fixture_ids': all_fixture_ids
        }


def process_single_h2h(
    fixture_id:   int,
    home_team_id: int,
    away_team_id: int
) -> dict:
    """Process head-to-head data for a single fixture. Saves last 5 only."""

    if not isinstance(fixture_id, int)   or fixture_id   <= 0:
        raise ValueError(f"Invalid fixture_id: {fixture_id}")
    if not isinstance(home_team_id, int) or home_team_id <= 0:
        raise ValueError(f"Invalid home_team_id: {home_team_id}")
    if not isinstance(away_team_id, int) or away_team_id <= 0:
        raise ValueError(f"Invalid away_team_id: {away_team_id}")

    cache_key = f"h2h_{fixture_id}_{home_team_id}_{away_team_id}"

    if cache.get(cache_key):
        logger.debug(f"H2H already processed for fixture {fixture_id}")
        return {"status": "cached", "fixture_id": fixture_id}

    from football.api_client import get_fixture_head_to_head
    h2h_data = get_fixture_head_to_head(f"{home_team_id}-{away_team_id}")
    

    if (
        not h2h_data or
        'response' not in h2h_data or
        not h2h_data['response'] or
        h2h_data.get('results', 0) == 0
    ):
        logger.info(f"No H2H data found for fixture {fixture_id}")
        cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
        return {"status": "no_data", "fixture_id": fixture_id}

    with transaction.atomic():
        try:
            fixture = Fixture.objects.get(id=fixture_id)
        except Fixture.DoesNotExist:
            logger.warning(f"Fixture {fixture_id} not found in database")
            raise ValueError(f"Fixture {fixture_id} does not exist")

        # ── Only save last 5 H2H matches — view only shows 5 ─────────────
        h2h_response  = h2h_data['response'][-5:]
        h2h_matches   = []
        skipped_count = 0

        for h in h2h_response:
            try:
                if not all([
                    h.get('fixture', {}).get('id'),
                    h.get('fixture', {}).get('date'),
                    h.get('teams',   {}).get('home', {}).get('name'),
                    h.get('teams',   {}).get('away', {}).get('name'),
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
                        home_extra_time_goals=(
                            h.get('score', {}).get('extratime', {}).get('home')
                            if h.get('score', {}).get('extratime') else None
                        ),
                        away_extra_time_goals=(
                            h.get('score', {}).get('extratime', {}).get('away')
                            if h.get('score', {}).get('extratime') else None
                        ),
                        home_penalty_goals=(
                            h.get('score', {}).get('penalty', {}).get('home')
                            if h.get('score', {}).get('penalty') else None
                        ),
                        away_penalty_goals=(
                            h.get('score', {}).get('penalty', {}).get('away')
                            if h.get('score', {}).get('penalty') else None
                        ),
                    )
                )
            except (KeyError, TypeError) as e:
                logger.warning(
                    f"Skipping malformed H2H match for fixture {fixture_id}: {e}"
                )
                skipped_count += 1
                continue

        if h2h_matches:
            HeadToHeadMatch.objects.bulk_create(
                h2h_matches,
                batch_size=BULK_CREATE_BATCH_SIZE,
                ignore_conflicts=True
            )
            logger.info(
                f"Processed H2H for fixture {fixture_id}: "
                f"{len(h2h_matches)} matches, {skipped_count} skipped"
            )
            cache.set(cache_key, True, timeout=CACHE_TIMEOUT_SUCCESS)
            return {
                "status":            "success",
                "fixture_id":        fixture_id,
                "matches_processed": len(h2h_matches),
                "skipped":           skipped_count
            }
        else:
            logger.warning(
                f"No valid H2H data for fixture {fixture_id}, "
                f"{skipped_count} records skipped"
            )
            cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
            return {
                "status":     "no_valid_data",
                "fixture_id": fixture_id,
                "skipped":    skipped_count
            }


def process_single_team_form(team_id: int, season: int) -> dict:
    """Process and save team previous form snapshots."""

    if not isinstance(team_id, int) or team_id <= 0:
        raise ValueError(f"Invalid team_id: {team_id}")
    if not isinstance(season, int) or season < 1900 or season > 2100:
        raise ValueError(f"Invalid season: {season}")

    cache_key = f"team_form_{team_id}_{season}"
    if cache.get(cache_key):
        logger.debug(
            f"Team form already processed for team {team_id} season {season}"
        )
        return {"status": "cached", "team_id": team_id, "season": season}

    try:
        team = Team.objects.get(id=team_id)
    except Team.DoesNotExist:
        logger.warning(f"Team {team_id} not found in database")
        raise ValueError(f"Team {team_id} does not exist")

    from football.api_client import get_fixtures
    previous_fixtures = get_fixtures(
        team_id=team_id, season=season, status='FT-AET-PEN'
    )

    if (
        not previous_fixtures or
        'response' not in previous_fixtures or
        not previous_fixtures['response']
    ):
        logger.info(
            f"No previous fixtures found for team {team_id} season {season}"
        )
        cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
        return {"status": "no_data", "team_id": team_id, "season": season}

    fixtures = previous_fixtures['response']

    opponent_ids = set()
    for fixture in fixtures:
        try:
            is_home     = fixture.get('teams', {}).get('home', {}).get('id') == team_id
            opponent_id = (
                fixture.get('teams', {}).get('away', {}).get('id') if is_home
                else fixture.get('teams', {}).get('home', {}).get('id')
            )
            if opponent_id:
                opponent_ids.add(opponent_id)
        except (KeyError, TypeError, AttributeError):
            continue

    opponents = {t.id: t for t in Team.objects.filter(id__in=opponent_ids)}

    missing_opponents = opponent_ids - opponents.keys()
    if missing_opponents:
        logger.warning(
            f"Missing opponent teams for team {team_id}: {missing_opponents}"
        )

    with transaction.atomic():
        form_snapshots = []
        skipped_count  = 0

        for fixture in fixtures:
            try:
                is_home    = fixture.get('teams', {}).get('home', {}).get('id') == team_id
                home_goals = fixture.get('goals', {}).get('home')
                away_goals = fixture.get('goals', {}).get('away')

                if home_goals is None or away_goals is None:
                    skipped_count += 1
                    continue

                if is_home:
                    result = (
                        'W' if home_goals > away_goals else
                        'L' if home_goals < away_goals else 'D'
                    )
                else:
                    result = (
                        'W' if away_goals > home_goals else
                        'L' if away_goals < home_goals else 'D'
                    )

                opponent_id   = (
                    fixture.get('teams', {}).get('away', {}).get('id') if is_home
                    else fixture.get('teams', {}).get('home', {}).get('id')
                )
                opponent_team = opponents.get(opponent_id)

                if not opponent_team:
                    logger.warning(
                        f"Opponent {opponent_id} not found for fixture "
                        f"{fixture.get('fixture', {}).get('id')}"
                    )
                    skipped_count += 1
                    continue

                form_snapshots.append(
                    TeamFormSnapshot(
                        team=team,
                        league_name=fixture.get('league', {}).get('name'),
                        league_id=fixture.get('league', {}).get('id'),
                        season=season,
                        fixture_id=fixture.get('fixture', {}).get('id'),
                        match_date=format_date_time(
                            fixture.get('fixture', {}).get('date')
                        ),
                        is_home=is_home,
                        opponent=opponent_team,
                        home_fulltime_goals=fixture.get('score', {}).get('fulltime', {}).get('home'),
                        away_fulltime_goals=fixture.get('score', {}).get('fulltime', {}).get('away'),
                        home_half_time_goals=fixture.get('score', {}).get('halftime', {}).get('home'),
                        away_half_time_goals=fixture.get('score', {}).get('halftime', {}).get('away'),
                        home_extra_time_goals=(
                            fixture.get('score', {}).get('extratime', {}).get('home')
                            if fixture.get('score', {}).get('extratime') else None
                        ),
                        away_extra_time_goals=(
                            fixture.get('score', {}).get('extratime', {}).get('away')
                            if fixture.get('score', {}).get('extratime') else None
                        ),
                        home_penalty_goals=(
                            fixture.get('score', {}).get('penalty', {}).get('home')
                            if fixture.get('score', {}).get('penalty') else None
                        ),
                        away_penalty_goals=(
                            fixture.get('score', {}).get('penalty', {}).get('away')
                            if fixture.get('score', {}).get('penalty') else None
                        ),
                        result=result,
                        round_name=fixture.get('league', {}).get('round')
                    )
                )

            except (KeyError, TypeError, AttributeError) as e:
                logger.warning(
                    f"Skipping malformed fixture data for team {team_id}: {e}"
                )
                skipped_count += 1
                continue

        if form_snapshots:
            TeamFormSnapshot.objects.bulk_create(
                form_snapshots,
                batch_size=BULK_CREATE_BATCH_SIZE,
                ignore_conflicts=True
            )
            logger.info(
                f"Processed form for team {team_id} season {season}: "
                f"{len(form_snapshots)} fixtures, {skipped_count} skipped"
            )
            cache.set(cache_key, True, timeout=CACHE_TIMEOUT_SUCCESS)
            return {
                "status":             "success",
                "team_id":            team_id,
                "season":             season,
                "fixtures_processed": len(form_snapshots),
                "skipped":            skipped_count
            }
        else:
            logger.warning(
                f"No valid form snapshots for team {team_id} season {season}, "
                f"{skipped_count} skipped"
            )
            cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
            return {
                "status":  "no_valid_data",
                "team_id": team_id,
                "season":  season,
                "skipped": skipped_count
            }


def process_single_league_standings(league_id: int, season: int) -> dict:
    """Update standings for a specific league and season."""

    if not isinstance(league_id, int) or league_id <= 0:
        raise ValueError(f"Invalid league_id: {league_id}")
    if not isinstance(season, int) or season < 1900 or season > 2100:
        raise ValueError(f"Invalid season: {season}")

    cache_key = f"standings_{league_id}_{season}"
    if cache.get(cache_key):
        logger.debug(
            f"Standings already cached for league {league_id} season {season}"
        )
        return {"status": "cached", "league_id": league_id, "season": season}

    try:
        league = League.objects.get(id=league_id)
    except League.DoesNotExist:
        logger.warning(f"League {league_id} not found in database")
        raise ValueError(f"League {league_id} does not exist")

    logger.info(f"Processing standings for league {league_id} season {season}")

    from football.api_client import get_league_table
    response = get_league_table(league_id=league_id, season=season)

    if (
        not response or
        response.get('results', 0) == 0 or
        not isinstance(response.get('response'), list) or
        len(response.get('response', [])) == 0
    ):
        logger.error(
            f"Invalid or empty API response for league {league_id}"
        )
        cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
        return {"status": "no_data", "league_id": league_id, "season": season}

    try:
        standings = response["response"][0]["league"]["standings"][0]
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Malformed standings data for league {league_id}: {e}")
        raise ValueError(f"Malformed standings data: {e}")

    team_ids = []
    for t in standings:
        try:
            team_id = t.get('team', {}).get('id')
            if team_id:
                team_ids.append(team_id)
        except (KeyError, TypeError, AttributeError):
            continue

    logger.info(
        f"Found {len(team_ids)} teams in standings for league {league_id}"
    )

    teams_map = {
        team.id: team
        for team in Team.objects.filter(id__in=team_ids)
    }

    missing_team_ids = set(team_ids) - teams_map.keys()
    if missing_team_ids:
        logger.warning(
            f"Missing teams in DB for league {league_id}: {missing_team_ids}"
        )

    table_list    = []
    skipped_count = 0

    for team_data in standings:
        try:
            team_id = team_data.get('team', {}).get('id')
            if not team_id:
                skipped_count += 1
                continue

            team = teams_map.get(team_id)
            if not team:
                logger.warning(f"Skipping team {team_id} — not in database")
                skipped_count += 1
                continue

            if team_data.get('rank') is None or team_data.get('points') is None:
                logger.warning(
                    f"Skipping team {team_id} — missing rank or points"
                )
                skipped_count += 1
                continue

            table_list.append(LeagueTableSnapshot(
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
                    'played':        team_data.get('home', {}).get('played', 0),
                    'wins':          team_data.get('home', {}).get('win', 0),
                    'draws':         team_data.get('home', {}).get('draw', 0),
                    'losses':        team_data.get('home', {}).get('lose', 0),
                    'goals_for':     team_data.get('home', {}).get('goals', {}).get('for', 0),
                    'goals_against': team_data.get('home', {}).get('goals', {}).get('against', 0),
                },
                away_stat={
                    'played':        team_data.get('away', {}).get('played', 0),
                    'wins':          team_data.get('away', {}).get('win', 0),
                    'draws':         team_data.get('away', {}).get('draw', 0),
                    'losses':        team_data.get('away', {}).get('lose', 0),
                    'goals_for':     team_data.get('away', {}).get('goals', {}).get('for', 0),
                    'goals_against': team_data.get('away', {}).get('goals', {}).get('against', 0),
                }
            ))

        except (KeyError, TypeError, AttributeError) as e:
            logger.error(
                f"Error creating snapshot for team {team_id} "
                f"in league {league_id}: {e}"
            )
            skipped_count += 1
            continue

    if table_list:
        try:
            with transaction.atomic():
                existing_count = LeagueTableSnapshot.objects.filter(
                    league=league, season=season
                ).count()

                if existing_count > 0:
                    logger.info(
                        f"Deleting {existing_count} existing snapshots "
                        f"for league {league_id} season {season}"
                    )
                    LeagueTableSnapshot.objects.filter(
                        league=league, season=season
                    ).delete()

                created_snapshots = LeagueTableSnapshot.objects.bulk_create(
                    table_list,
                    batch_size=BULK_CREATE_BATCH_SIZE,
                    ignore_conflicts=True
                )

                # ── Use actual_count in log ────────────────────────────────
                actual_count = LeagueTableSnapshot.objects.filter(
                    league=league, season=season
                ).count()

                logger.info(
                    f"✅ Created {actual_count} snapshots "
                    f"for league {league_id} season {season} "
                    f"(bulk_create returned {len(created_snapshots)})"
                )

                cache.set(cache_key, True, timeout=CACHE_TIMEOUT_SUCCESS)

                return {
                    "status":          "success",
                    "league_id":       league_id,
                    "season":          season,
                    "teams_processed": actual_count,
                    "skipped":         skipped_count
                }

        except Exception as e:
            logger.error(
                f"DB error creating snapshots for league {league_id}: {e}",
                exc_info=True
            )
            raise
    else:
        logger.warning(
            f"No valid snapshots for league {league_id}, "
            f"{skipped_count} teams skipped"
        )
        cache.set(cache_key, True, timeout=CACHE_TIMEOUT_NO_DATA)
        return {
            "status":    "no_valid_data",
            "league_id": league_id,
            "season":    season,
            "skipped":   skipped_count
        }


def compute_advanced_fixture_stats(fixture) -> None:
    """
    Compute advanced statistics from TeamFormSnapshot data.
    Raises ValueError if form data is missing so the caller
    can retry rather than silently marking the fixture as done.
    """
    home_team   = fixture.home_team
    away_team   = fixture.away_team
    season      = fixture.league.season
    league_type = fixture.league.type.lower() if fixture.league.type else 'league'

    home_snapshots = TeamFormSnapshot.objects.filter(
        team=home_team, season=season
    ).select_related('opponent').order_by('-match_date')

    away_snapshots = TeamFormSnapshot.objects.filter(
        team=away_team, season=season
    ).select_related('opponent').order_by('-match_date')

    # ── Raise instead of silent return so caller retries ─────────────────
    if not home_snapshots.exists() or not away_snapshots.exists():
        raise ValueError(
            f"No form data for fixture {fixture.id} — "
            f"home exists: {home_snapshots.exists()}, "
            f"away exists: {away_snapshots.exists()}"
        )

    def build_match_detail(snapshot):
        if snapshot.is_home:
            fulltime_scored   = snapshot.home_fulltime_goals or 0
            fulltime_conceded = snapshot.away_fulltime_goals or 0
            halftime_scored   = snapshot.home_half_time_goals
            halftime_conceded = snapshot.away_half_time_goals
            extratime_scored  = snapshot.home_extra_time_goals
            extratime_conceded = snapshot.away_extra_time_goals
            penalty_scored    = snapshot.home_penalty_goals
            penalty_conceded  = snapshot.away_penalty_goals
        else:
            fulltime_scored   = snapshot.away_fulltime_goals or 0
            fulltime_conceded = snapshot.home_fulltime_goals or 0
            halftime_scored   = snapshot.away_half_time_goals
            halftime_conceded = snapshot.home_half_time_goals
            extratime_scored  = snapshot.away_extra_time_goals
            extratime_conceded = snapshot.home_extra_time_goals
            penalty_scored    = snapshot.away_penalty_goals
            penalty_conceded  = snapshot.home_penalty_goals

        return {
            'result':             snapshot.result,
            'is_home':            snapshot.is_home,
            'opponent':           snapshot.opponent.name if snapshot.opponent else 'Unknown',
            'goals_scored':       fulltime_scored,
            'goals_conceded':     fulltime_conceded,
            'halftime_scored':    halftime_scored,
            'halftime_conceded':  halftime_conceded,
            'extratime_scored':   extratime_scored,
            'extratime_conceded': extratime_conceded,
            'penalty_scored':     penalty_scored,
            'penalty_conceded':   penalty_conceded,
            'date':               snapshot.match_date.strftime('%Y-%m-%d') if snapshot.match_date else None,
            'fixture_id':         snapshot.fixture_id
        }

    # 1. Overall form (last 5 any venue)
    home_last_5 = list(home_snapshots[:5])
    away_last_5 = list(away_snapshots[:5])

    home_last_5_form = [build_match_detail(s) for s in home_last_5]
    away_last_5_form = [build_match_detail(s) for s in away_last_5]

    home_wins_last_5   = sum(1 for s in home_last_5 if s.result == 'W')
    home_draws_last_5  = sum(1 for s in home_last_5 if s.result == 'D')
    home_losses_last_5 = sum(1 for s in home_last_5 if s.result == 'L')

    away_wins_last_5   = sum(1 for s in away_last_5 if s.result == 'W')
    away_draws_last_5  = sum(1 for s in away_last_5 if s.result == 'D')
    away_losses_last_5 = sum(1 for s in away_last_5 if s.result == 'L')

    home_goals_scored_last_5   = 0
    home_goals_conceded_last_5 = 0
    for s in home_last_5:
        if s.is_home:
            home_goals_scored_last_5   += s.home_fulltime_goals or 0
            home_goals_conceded_last_5 += s.away_fulltime_goals or 0
        else:
            home_goals_scored_last_5   += s.away_fulltime_goals or 0
            home_goals_conceded_last_5 += s.home_fulltime_goals or 0

    away_goals_scored_last_5   = 0
    away_goals_conceded_last_5 = 0
    for s in away_last_5:
        if s.is_home:
            away_goals_scored_last_5   += s.home_fulltime_goals or 0
            away_goals_conceded_last_5 += s.away_fulltime_goals or 0
        else:
            away_goals_scored_last_5   += s.away_fulltime_goals or 0
            away_goals_conceded_last_5 += s.home_fulltime_goals or 0

    # 2. Home/Away specific form
    home_home_matches = list(home_snapshots.filter(is_home=True)[:5])
    away_away_matches = list(away_snapshots.filter(is_home=False)[:5])

    home_last_5_home_form = [build_match_detail(s) for s in home_home_matches]
    away_last_5_away_form = [build_match_detail(s) for s in away_away_matches]

    home_home_wins_last_5   = sum(1 for s in home_home_matches if s.result == 'W')
    home_home_draws_last_5  = sum(1 for s in home_home_matches if s.result == 'D')
    home_home_losses_last_5 = sum(1 for s in home_home_matches if s.result == 'L')

    away_away_wins_last_5   = sum(1 for s in away_away_matches if s.result == 'W')
    away_away_draws_last_5  = sum(1 for s in away_away_matches if s.result == 'D')
    away_away_losses_last_5 = sum(1 for s in away_away_matches if s.result == 'L')

    # 3. Similar rank / cup competition
    is_cup_competition = 'cup' in league_type

    try:
        if is_cup_competition:
            logger.info(
                f"Cup competition: {fixture.league.name} — filtering by league_name"
            )
            home_similar_matches = home_snapshots.filter(
                league_name=fixture.league.name
            )[:5]
            away_similar_matches = away_snapshots.filter(
                league_name=fixture.league.name
            )[:5]
            home_similar = [build_match_detail(s) for s in home_similar_matches]
            away_similar = [build_match_detail(s) for s in away_similar_matches]

        else:
            logger.info(
                f"League competition: {fixture.league.name} — filtering by similar rank"
            )
            home_standing = LeagueTableSnapshot.objects.filter(
                team=home_team, league=fixture.league, season=season
            ).first()

            away_standing = LeagueTableSnapshot.objects.filter(
                team=away_team, league=fixture.league, season=season
            ).first()

            home_similar = []
            if away_standing and away_standing.rank:
                similar_ids = LeagueTableSnapshot.objects.filter(
                    league=fixture.league,
                    season=season,
                    rank__gte=max(1, away_standing.rank - 3),
                    rank__lte=away_standing.rank + 3
                ).values_list('team_id', flat=True)

                home_similar = [
                    build_match_detail(s)
                    for s in home_snapshots.filter(
                        opponent_id__in=similar_ids
                    )[:5]
                ]

            away_similar = []
            if home_standing and home_standing.rank:
                similar_ids = LeagueTableSnapshot.objects.filter(
                    league=fixture.league,
                    season=season,
                    rank__gte=max(1, home_standing.rank - 3),
                    rank__lte=home_standing.rank + 3
                ).values_list('team_id', flat=True)

                away_similar = [
                    build_match_detail(s)
                    for s in away_snapshots.filter(
                        opponent_id__in=similar_ids
                    )[:5]
                ]

        placeholder = 'No cup data' if is_cup_competition else 'No data'
        if not home_similar:
            home_similar = [{
                'result': 'D', 'is_home': False, 'opponent': placeholder,
                'goals_scored': 0, 'goals_conceded': 0
            }]
        if not away_similar:
            away_similar = [{
                'result': 'D', 'is_home': False, 'opponent': placeholder,
                'goals_scored': 0, 'goals_conceded': 0
            }]

    except Exception as e:
        logger.error(f"Error computing similar rank/cup stats: {e}")
        home_similar = [{
            'result': 'D', 'is_home': False,
            'opponent': 'No data', 'goals_scored': 0, 'goals_conceded': 0
        }]
        away_similar = [{
            'result': 'D', 'is_home': False,
            'opponent': 'No data', 'goals_scored': 0, 'goals_conceded': 0
        }]

    FixtureAdvancedStats.objects.update_or_create(
        fixture=fixture,
        defaults={
            'home_last_5_form':             home_last_5_form,
            'away_last_5_form':             away_last_5_form,
            'home_wins_last_5':             home_wins_last_5,
            'home_draws_last_5':            home_draws_last_5,
            'home_losses_last_5':           home_losses_last_5,
            'home_goals_scored_last_5':     home_goals_scored_last_5,
            'home_goals_conceded_last_5':   home_goals_conceded_last_5,
            'away_wins_last_5':             away_wins_last_5,
            'away_draws_last_5':            away_draws_last_5,
            'away_losses_last_5':           away_losses_last_5,
            'away_goals_scored_last_5':     away_goals_scored_last_5,
            'away_goals_conceded_last_5':   away_goals_conceded_last_5,
            'home_last_5_home_form':        home_last_5_home_form,
            'away_last_5_away_form':        away_last_5_away_form,
            'home_home_wins_last_5':        home_home_wins_last_5,
            'home_home_draws_last_5':       home_home_draws_last_5,
            'home_home_losses_last_5':      home_home_losses_last_5,
            'away_away_wins_last_5':        away_away_wins_last_5,
            'away_away_draws_last_5':       away_away_draws_last_5,
            'away_away_losses_last_5':      away_away_losses_last_5,
            'home_last_5_vs_similar_rank':  home_similar,
            'away_last_5_vs_similar_rank':  away_similar,
        }
    )

    logger.info(
        f"✅ Advanced stats computed for fixture {fixture.id} ({league_type})"
    )



def process_single_fixture_stats(
    match_fixture_id:  int,
    parent_fixture_id: int             = None,
    date:              Optional[date_type] = None,
    score_details:     dict            = None
) -> dict:
    """
    Process detailed statistics for a single past match fixture.
    Returns a status dict — never raises — so the caller can handle
    missing fixtures gracefully without counting them as failures.

    match_fixture_id  — the past match we want stats for (API call + PK in FixtureStatistics)
    parent_fixture_id — the upcoming fixture that referenced this past match
    """
    if not isinstance(parent_fixture_id, int) or parent_fixture_id <= 0:
        raise ValueError(f"Invalid parent_fixture_id: {parent_fixture_id}")

    from football.api_client import get_match_stats

    # ── Parent fixture must exist — it owns the FK relationship ──────────
    try:
        parent_fixture = Fixture.objects.get(id=parent_fixture_id)
    except Fixture.DoesNotExist:
        logger.warning(
            f"Parent fixture {parent_fixture_id} not in DB — skipping"
        )
        return {"status": "no_fixture", "fixture_id": parent_fixture_id}

    logger.info(
        f"🔄 Processing detailed stats for past match {match_fixture_id} "
        f"(parent: {parent_fixture_id})"
    )

    # ── Fetch stats for the PAST match, not the parent ───────────────────
    stats_data = get_match_stats(match_fixture_id)
    if not stats_data or 'response' not in stats_data or not stats_data['response']:
        logger.warning(f"⚠️ No statistics data for fixture {match_fixture_id}")
        return {"status": "no_data", "fixture_id": match_fixture_id}

    teams_stats = stats_data['response']

    if len(teams_stats) != 2:
        logger.warning(
            f"⚠️ Expected 2 teams, got {len(teams_stats)} "
            f"for fixture {match_fixture_id}"
        )
        return {"status": "invalid_data", "fixture_id": match_fixture_id}

    def parse_team_stats(team_data):
        stats_dict = {}
        for stat in team_data.get('statistics', []):
            stats_dict[stat.get('type')] = stat.get('value')
        return stats_dict

    def safe_int(value):
        if value is None:
            return None
        if isinstance(value, str):
            value = value.replace('%', '').strip()
            if not value:
                return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def safe_decimal(value):
        if value is None:
            return None
        try:
            from decimal import Decimal
            return Decimal(str(value))
        except Exception:
            return None

    if score_details:
        home_fulltime  = score_details.get('goals_scored')
        away_fulltime  = score_details.get('goals_conceded')
        home_halftime  = score_details.get('halftime_scored')
        away_halftime  = score_details.get('halftime_conceded')
        home_extratime = score_details.get('extratime_scored')
        away_extratime = score_details.get('extratime_conceded')
        home_penalty   = score_details.get('penalty_scored')
        away_penalty   = score_details.get('penalty_conceded')

        if not score_details.get('is_home', True):
            home_fulltime,  away_fulltime  = away_fulltime,  home_fulltime
            home_halftime,  away_halftime  = away_halftime,  home_halftime
            home_extratime, away_extratime = away_extratime, home_extratime
            home_penalty,   away_penalty   = away_penalty,   home_penalty
    else:
        home_fulltime  = getattr(parent_fixture, 'home_goals', None)
        away_fulltime  = getattr(parent_fixture, 'away_goals', None)
        home_halftime  = None
        away_halftime  = None
        home_extratime = None
        away_extratime = None
        home_penalty   = None
        away_penalty   = None

    team1       = teams_stats[0]
    team1_stats = parse_team_stats(team1)
    team2       = teams_stats[1]
    team2_stats = parse_team_stats(team2)

    with transaction.atomic():
        fixture_stat, created = FixtureStatistics.objects.update_or_create(
            # ── Keyed on past match ID — one row per past match ──────────
            match_id=match_fixture_id,
            defaults={
                'fixture':               parent_fixture,  # FK to parent
                'date':                  date,
                'home_fulltime_goals':   home_fulltime,
                'away_fulltime_goals':   away_fulltime,
                'home_half_time_goals':  home_halftime,
                'away_half_time_goals':  away_halftime,
                'home_extra_time_goals': home_extratime,
                'away_extra_time_goals': away_extratime,
                'home_penalty_goals':    home_penalty,
                'away_penalty_goals':    away_penalty,

                'home_team_id':              team1['team']['id'],
                'home_team_name':            team1['team']['name'],
                'home_shots_on_goal':        safe_int(team1_stats.get('Shots on Goal')),
                'home_shots_off_goal':       safe_int(team1_stats.get('Shots off Goal')),
                'home_total_shots':          safe_int(team1_stats.get('Total Shots')),
                'home_blocked_shots':        safe_int(team1_stats.get('Blocked Shots')),
                'home_shots_insidebox':      safe_int(team1_stats.get('Shots insidebox')),
                'home_shots_outsidebox':     safe_int(team1_stats.get('Shots outsidebox')),
                'home_fouls':                safe_int(team1_stats.get('Fouls')),
                'home_corner_kicks':         safe_int(team1_stats.get('Corner Kicks')),
                'home_offsides':             safe_int(team1_stats.get('Offsides')),
                'home_ball_possession':      safe_int(team1_stats.get('Ball Possession')),
                'home_yellow_cards':         safe_int(team1_stats.get('Yellow Cards')),
                'home_red_cards':            safe_int(team1_stats.get('Red Cards')),
                'home_goalkeeper_saves':     safe_int(team1_stats.get('Goalkeeper Saves')),
                'home_total_passes':         safe_int(team1_stats.get('Total passes')),
                'home_passes_accurate':      safe_int(team1_stats.get('Passes accurate')),
                'home_passes_percentage':    safe_int(team1_stats.get('Passes %')),
                'home_expected_goals':       safe_decimal(team1_stats.get('expected_goals')),
                'home_goals_prevented':      safe_int(team1_stats.get('goals_prevented')),

                'away_team_id':              team2['team']['id'],
                'away_team_name':            team2['team']['name'],
                'away_shots_on_goal':        safe_int(team2_stats.get('Shots on Goal')),
                'away_shots_off_goal':       safe_int(team2_stats.get('Shots off Goal')),
                'away_total_shots':          safe_int(team2_stats.get('Total Shots')),
                'away_blocked_shots':        safe_int(team2_stats.get('Blocked Shots')),
                'away_shots_insidebox':      safe_int(team2_stats.get('Shots insidebox')),
                'away_shots_outsidebox':     safe_int(team2_stats.get('Shots outsidebox')),
                'away_fouls':                safe_int(team2_stats.get('Fouls')),
                'away_corner_kicks':         safe_int(team2_stats.get('Corner Kicks')),
                'away_offsides':             safe_int(team2_stats.get('Offsides')),
                'away_ball_possession':      safe_int(team2_stats.get('Ball Possession')),
                'away_yellow_cards':         safe_int(team2_stats.get('Yellow Cards')),
                'away_red_cards':            safe_int(team2_stats.get('Red Cards')),
                'away_goalkeeper_saves':     safe_int(team2_stats.get('Goalkeeper Saves')),
                'away_total_passes':         safe_int(team2_stats.get('Total passes')),
                'away_passes_accurate':      safe_int(team2_stats.get('Passes accurate')),
                'away_passes_percentage':    safe_int(team2_stats.get('Passes %')),
                'away_expected_goals':       safe_decimal(team2_stats.get('expected_goals')),
                'away_goals_prevented':      safe_int(team2_stats.get('goals_prevented')),
            }
        )

    action = "created" if created else "updated"
    logger.info(
        f"✅ Detailed stats {action} for past match {match_fixture_id} "
        f"(parent: {parent_fixture_id})"
    )

    return {
        "status":            "success",
        "fixture_id":        match_fixture_id,
        "parent_fixture_id": parent_fixture_id,
        "action":            action
    }