import time
from celery import shared_task
from django.utils import timezone
from django.core.cache import cache
from datetime import datetime, timedelta
import logging

from football.models import (
    Fixture,
    FixtureIngestion,
    FixtureAdvancedStats,
    FixtureStatistics,
    HeadToHeadMatch,
)
from football.api_client import RateLimitExceeded

logger = logging.getLogger(__name__)

# ── Batch sizes ───────────────────────────────────────────────────────────────
H2H_BATCH_SIZE            = 400
FORM_BATCH_SIZE           = 100
STANDINGS_BATCH_SIZE      = 30
ADVANCED_STATS_BATCH_SIZE = 1000
DETAILED_STATS_BATCH_SIZE = 35

MAX_RETRY_COUNT = 5


# ── DAILY INGESTION ──────────────────────────────────────────────────────────

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def ingest_next_day_fixtures(self):
    """Daily task — fetch fixtures for Day 6. Runs at 2 AM."""
    try:
        target_date = datetime.now() + timedelta(days=6)
        date_str    = target_date.strftime('%Y-%m-%d')
        logger.info(f"🚀 Daily ingestion: Fetching fixtures for {date_str}")

        from football.tasks import fetch_and_process_day
        result = fetch_and_process_day(date_str)

        logger.info(
            f"✅ Daily ingestion complete for {date_str}: "
            f"{result['created']} created, {result['updated']} updated"
        )
        return {
            'status':         'success',
            'date':           date_str,
            'total_fixtures': result['created'] + result['updated'],
            'created':        result['created'],
            'updated':        result['updated'],
            'skipped':        result['skipped'],
            'fixture_ids':    result['fixture_ids'],
            'timestamp':      timezone.now().isoformat()
        }

    except RateLimitExceeded as exc:
        logger.warning(f"⚠️ Rate limit hit, retrying in {exc.wait_time}s")
        raise self.retry(countdown=exc.wait_time, exc=exc)
    except Exception as exc:
        logger.error(f"❌ Daily ingestion failed: {exc}", exc_info=True)
        raise self.retry(countdown=300, exc=exc)


# ── STAGE 1A: H2H ────────────────────────────────────────────────────────────

@shared_task(bind=True, max_retries=3)
def process_h2h_batch(self):
    """
    1 API call per fixture.
    Batch: 400 | Calls: 400 | Time: ~140s | Window: 10 min
    Crontab: 0,10,20,30,40,50
    """
    try:
        ingestions = FixtureIngestion.objects.filter(
            needs_h2h=True,
            h2h_retry_count__lt=MAX_RETRY_COUNT
        ).select_related(
            'fixture',
            'fixture__home_team',
            'fixture__away_team',
            'fixture__league',
        ).order_by('fixture__league__priority')[:H2H_BATCH_SIZE]

        if not ingestions:
            return {"status": "no_work", "processed": 0}

        logger.info(f"🔄 H2H batch: {len(ingestions)} fixtures")

        processed = 0

        for ingestion in ingestions:
            try:
                from football.tasks import process_single_h2h

                result = process_single_h2h(
                    ingestion.fixture.id,
                    ingestion.fixture.home_team.id,
                    ingestion.fixture.away_team.id,
                )

                if result.get('status') in [
                    'success', 'cached', 'no_data', 'no_valid_data'
                ]:
                    ingestion.needs_h2h        = False
                    ingestion.h2h_processed_at = timezone.now()
                    ingestion.save(update_fields=[
                        'needs_h2h', 'h2h_processed_at', 'updated_at'
                    ])
                    ingestion.refresh_from_db()
                    ingestion.check_and_mark_complete()
                    processed += 1

            except RateLimitExceeded as exc:
                logger.warning(
                    f"⚠️ Rate limit hit H2H fixture {ingestion.fixture.id}, "
                    f"sleeping {exc.wait_time:.2f}s then continuing"
                )
                time.sleep(exc.wait_time)

            except Exception as exc:
                logger.error(
                    f"❌ H2H error fixture {ingestion.fixture.id}: {exc}"
                )
                ingestion.h2h_retry_count += 1
                ingestion.last_error = str(exc)[:500]
                ingestion.save(update_fields=[
                    'h2h_retry_count', 'last_error', 'updated_at'
                ])

        remaining = FixtureIngestion.objects.filter(needs_h2h=True).count()
        logger.info(f"✅ H2H: {processed} processed, {remaining} remaining")

        return {
            "status":    "success",
            "processed": processed,
            "remaining": remaining
        }

    except Exception as exc:
        logger.error(f"H2H batch task error: {exc}")
        raise self.retry(countdown=120, exc=exc)


# ── STAGE 1B: FORM ───────────────────────────────────────────────────────────

@shared_task(bind=True, max_retries=3)
def process_form_batch(self):
    """
    2 API calls per fixture (home + away team).
    Batch: 100 fixtures = ~200 calls | Time: ~70s | Window: 8 min
    Crontab: 1,9,17,25,33,41,49,57
    """
    try:
        ingestions = FixtureIngestion.objects.filter(
            needs_form=True,
            form_retry_count__lt=MAX_RETRY_COUNT
        ).select_related(
            'fixture__home_team',
            'fixture__away_team',
            'fixture__league'
        ).order_by('fixture__league__priority')[:FORM_BATCH_SIZE]

        if not ingestions:
            return {"status": "no_work", "processed": 0}

        teams_to_process = set()
        ingestion_map    = {}

        for ing in ingestions:
            home_key = (ing.fixture.home_team.id, ing.fixture.league.season)
            away_key = (ing.fixture.away_team.id, ing.fixture.league.season)
            teams_to_process.add(home_key)
            teams_to_process.add(away_key)

            if ing.fixture.id not in ingestion_map:
                ingestion_map[ing.fixture.id] = {
                    'ingestion':  ing,
                    'needs_home': home_key,
                    'needs_away': away_key
                }

        logger.info(
            f"🔄 Form batch: {len(teams_to_process)} unique teams "
            f"from {len(ingestions)} fixtures"
        )

        processed       = 0
        processed_teams = set()

        for team_id, season in list(teams_to_process)[:FORM_BATCH_SIZE * 2]:
            try:
                from football.tasks import process_single_team_form

                result = process_single_team_form(team_id, season)

                if result.get('status') in [
                    'success', 'cached', 'no_data', 'no_valid_data'
                ]:
                    processed_teams.add((team_id, season))
                    processed += 1

            except RateLimitExceeded as exc:
                logger.warning(
                    f"⚠️ Rate limit hit form team {team_id}, "
                    f"sleeping {exc.wait_time:.2f}s then continuing"
                )
                time.sleep(exc.wait_time)

            except Exception as exc:
                logger.error(f"❌ Form error team {team_id}: {exc}")

        if processed_teams:
            for fixture_id, data in ingestion_map.items():
                if (
                    data['needs_home'] in processed_teams and
                    data['needs_away'] in processed_teams
                ):
                    ing                   = data['ingestion']
                    ing.needs_form        = False
                    ing.form_processed_at = timezone.now()
                    ing.save(update_fields=[
                        'needs_form', 'form_processed_at', 'updated_at'
                    ])
                    ing.refresh_from_db()
                    ing.check_and_mark_complete()

        remaining = FixtureIngestion.objects.filter(needs_form=True).count()
        logger.info(
            f"✅ Form: {processed} teams done, {remaining} fixtures remaining"
        )

        return {
            "status":    "success",
            "processed": processed,
            "remaining": remaining
        }

    except Exception as exc:
        logger.error(f"Form batch task error: {exc}")
        raise self.retry(countdown=300, exc=exc)


# ── STAGE 1C: STANDINGS ──────────────────────────────────────────────────────

@shared_task(bind=True, max_retries=3)
def process_standings_batch(self):
    """
    1 API call per unique league (~15 per batch).
    Batch: 30 fixtures = ~15 calls | Time: ~5s | Window: 5 min
    Crontab: 3,13,18,23,28,33,38,43,48,53,58
    """
    try:
        ingestions = FixtureIngestion.objects.filter(
            needs_standings=True,
            standings_retry_count__lt=MAX_RETRY_COUNT
        ).select_related('fixture__league').order_by('fixture__league__priority')[:STANDINGS_BATCH_SIZE * 2]

        if not ingestions:
            return {"status": "no_work", "processed": 0}

        leagues_to_process   = set()
        league_ingestion_map = {}

        for ing in ingestions:
            league_key = (ing.fixture.league.id, ing.fixture.league.season)
            leagues_to_process.add(league_key)

            if league_key not in league_ingestion_map:
                league_ingestion_map[league_key] = []
            league_ingestion_map[league_key].append(ing)

            if len(leagues_to_process) >= STANDINGS_BATCH_SIZE:
                break

        logger.info(
            f"🔄 Standings batch: {len(leagues_to_process)} unique leagues"
        )

        processed         = 0
        processed_leagues = set()

        for league_id, season in list(leagues_to_process)[:STANDINGS_BATCH_SIZE]:
            try:
                from football.tasks import process_single_league_standings

                result = process_single_league_standings(league_id, season)

                if result.get('status') in [
                    'success', 'cached', 'no_data', 'no_valid_data'
                ]:
                    processed_leagues.add((league_id, season))
                    processed += 1

            except RateLimitExceeded as exc:
                logger.warning(
                    f"⚠️ Rate limit hit standings league {league_id}, "
                    f"sleeping {exc.wait_time:.2f}s then continuing"
                )
                time.sleep(exc.wait_time)

            except Exception as exc:
                logger.error(
                    f"❌ Standings error league {league_id}: {exc}"
                )

        if processed_leagues:
            for league_key in processed_leagues:
                for ing in league_ingestion_map.get(league_key, []):
                    ing.needs_standings        = False
                    ing.standings_processed_at = timezone.now()
                    ing.save(update_fields=[
                        'needs_standings', 'standings_processed_at', 'updated_at'
                    ])
                    ing.refresh_from_db()
                    ing.check_and_mark_complete()

        remaining = FixtureIngestion.objects.filter(needs_standings=True).count()
        logger.info(
            f"✅ Standings: {processed} leagues done, {remaining} fixtures remaining"
        )

        return {
            "status":    "success",
            "processed": processed,
            "remaining": remaining
        }

    except Exception as exc:
        logger.error(f"Standings batch task error: {exc}")
        raise self.retry(countdown=300, exc=exc)


# ── STAGE 2: ADVANCED STATS ──────────────────────────────────────────────────

@shared_task(bind=True, max_retries=3)
def process_advanced_stats_batch(self):
    """
    0 API calls — pure DB computation.
    Gate: needs_h2h=False AND needs_form=False AND needs_standings=False
    Batch: 1000 | Time: ~90s | Window: 5 min
    Crontab: 4,14,19,24,29,34,39,44,49,54,59
    """
    try:
        ingestions = FixtureIngestion.objects.filter(
            needs_advanced_stats=True,
            advanced_stats_retry_count__lt=MAX_RETRY_COUNT,
            needs_h2h=False,
            needs_form=False,
            needs_standings=False
        ).select_related('fixture__league').order_by('fixture__league__priority')[:ADVANCED_STATS_BATCH_SIZE]

        if not ingestions:
            return {"status": "no_work", "processed": 0}

        logger.info(
            f"🔄 Advanced stats: {len(ingestions)} fixtures (no API)"
        )

        success_ids = []
        failed_ids  = []

        for ingestion in ingestions:
            try:
                from football.tasks import compute_advanced_fixture_stats

                compute_advanced_fixture_stats(ingestion.fixture)
                success_ids.append(ingestion.fixture_id)

            except Exception as exc:
                logger.error(
                    f"❌ Advanced stats error fixture {ingestion.fixture.id}: {exc}"
                )
                failed_ids.append(ingestion.fixture_id)
                ingestion.advanced_stats_retry_count += 1
                ingestion.last_error = str(exc)[:500]
                ingestion.save(update_fields=[
                    'advanced_stats_retry_count', 'last_error', 'updated_at'
                ])

        if success_ids:
            FixtureIngestion.objects.filter(
                fixture_id__in=success_ids
            ).update(
                needs_advanced_stats=False,
                advanced_stats_processed_at=timezone.now()
            )

            FixtureIngestion.objects.filter(
                fixture_id__in=success_ids,
                needs_h2h=False,
                needs_form=False,
                needs_standings=False,
                needs_advanced_stats=False,
                needs_detailed_stats=False,
                is_fully_processed=False
            ).update(
                is_fully_processed=True,
                fully_processed_at=timezone.now()
            )

        remaining  = FixtureIngestion.objects.filter(
            needs_advanced_stats=True
        ).count()
        fully_done = FixtureIngestion.objects.filter(
            is_fully_processed=True
        ).count()

        logger.info(
            f"✅ Advanced stats: {len(success_ids)} done, "
            f"{len(failed_ids)} failed, "
            f"{remaining} remaining, {fully_done} fully ready"
        )

        return {
            "status":                "success",
            "processed":             len(success_ids),
            "failed":                len(failed_ids),
            "remaining":             remaining,
            "fully_processed_total": fully_done
        }

    except Exception as exc:
        logger.error(f"Advanced stats batch task error: {exc}")
        raise self.retry(countdown=300, exc=exc)


# ── STAGE 3: DETAILED STATS ──────────────────────────────────────────────────

@shared_task(bind=True, max_retries=3)
def process_detailed_stats_batch(self):
    """
    ~20 unique API calls per parent fixture (form + H2H matches).
    Gate: ALL of needs_h2h, needs_form, needs_standings, needs_advanced_stats
          must be False before a fixture is eligible.

    Historical matches from form JSON are NOT in the Fixture table —
    completion only checks FixtureStatistics, never Fixture table.
    Matches with no API data are tracked and treated as resolved.

    Batch: 35 parent fixtures | Cap: 500 calls | Time: ~175s | Window: 5 min
    Crontab: 2,5,8,11,16,21,26,31,36,41,46,51,56

    Lock prevents overlap between crontab triggers — without it a slow run
    would have a second run start mid-way causing duplicate API calls and
    race conditions on the completion check.
    """

    # ── LOCK ──────────────────────────────────────────────────────────────
    # cache.add() is atomic — returns True only if key didn't already exist
    # If it returns False, another run is in progress — skip this trigger
    lock_key     = 'detailed_stats_lock'
    lock_timeout = 300  # 5 min — matches crontab window

    if not cache.add(lock_key, 'locked', lock_timeout):
        logger.info("⏭️ Detailed stats already running, skipping this trigger")
        return {"status": "skipped", "reason": "already_running"}

    try:
        ingestions = FixtureIngestion.objects.filter(
            needs_detailed_stats=True,
            detailed_stats_retry_count__lt=MAX_RETRY_COUNT,
            needs_h2h=False,
            needs_form=False,
            needs_standings=False,
            needs_advanced_stats=False
        ).select_related('fixture__league').order_by('fixture__league__priority')[:DETAILED_STATS_BATCH_SIZE]

        if not ingestions.exists():
            logger.info(
                "No fixtures ready for detailed stats — "
                "Stage 1+2 not complete yet"
            )
            return {"status": "no_work", "processed": 0}

        logger.info(
            f"🔄 Detailed stats: {ingestions.count()} parent fixtures ready"
        )

        matches_to_process = []

        for ingestion in ingestions:

            try:
                adv = FixtureAdvancedStats.objects.get(
                    fixture=ingestion.fixture
                )

                all_forms = (
                    (adv.home_last_5_form            or []) +
                    (adv.away_last_5_form            or []) +
                    (adv.home_last_5_home_form        or []) +
                    (adv.away_last_5_away_form        or []) +
                    (adv.home_last_5_vs_similar_rank  or []) +
                    (adv.away_last_5_vs_similar_rank  or [])
                )

                for match_data in all_forms:
                    match_id = match_data.get('fixture_id')
                    if not match_id:
                        continue

                    match_date = match_data.get('date')
                    if not match_date:
                        continue
                    try:
                        parsed_date = datetime.strptime(
                            match_date, '%Y-%m-%d'
                        ).date()
                    except ValueError:
                        continue

                    matches_to_process.append((
                        ingestion.fixture.id,  # parent_fixture_id
                        match_id,              # fixture_id (past match)
                        parsed_date,
                        {
                            'goals_scored':       match_data.get('goals_scored'),
                            'goals_conceded':     match_data.get('goals_conceded'),
                            'halftime_scored':    match_data.get('halftime_scored'),
                            'halftime_conceded':  match_data.get('halftime_conceded'),
                            'extratime_scored':   match_data.get('extratime_scored'),
                            'extratime_conceded': match_data.get('extratime_conceded'),
                            'penalty_scored':     match_data.get('penalty_scored'),
                            'penalty_conceded':   match_data.get('penalty_conceded'),
                            'is_home':            match_data.get('is_home', True)
                        }
                    ))

            except FixtureAdvancedStats.DoesNotExist:
                logger.warning(
                    f"No advanced stats for fixture {ingestion.fixture_id}"
                )

            # ── Source 2: H2H matches ──────────────────────────────────────
            for h2h in HeadToHeadMatch.objects.filter(
                fixture=ingestion.fixture
            ):
                if not h2h.past_fixture_id:
                    continue

                matches_to_process.append((
                    ingestion.fixture.id,  # parent_fixture_id
                    h2h.past_fixture_id,   # fixture_id (past match)
                    h2h.date.date() if h2h.date else None,
                    {
                        'goals_scored':       h2h.home_fulltime_goals,
                        'goals_conceded':     h2h.away_fulltime_goals,
                        'halftime_scored':    h2h.home_half_time_goals,
                        'halftime_conceded':  h2h.away_half_time_goals,
                        'extratime_scored':   h2h.home_extra_time_goals,
                        'extratime_conceded': h2h.away_extra_time_goals,
                        'penalty_scored':     h2h.home_penalty_goals,
                        'penalty_conceded':   h2h.away_penalty_goals,
                        'is_home':            True
                    }
                ))

        seen           = {}
        unique_matches = []
        for parent_id, match_id, parsed_date, score_details in matches_to_process:
            if match_id not in seen:
                seen[match_id] = True
                unique_matches.append(
                    (parent_id, match_id, parsed_date, score_details)
                )

       
        existing_stats = set(
            FixtureStatistics.objects.filter(
                match_id__in=[m[1] for m in unique_matches]
            ).values_list('match_id', flat=True)
    )

        to_process = [  
            (parent_id, match_id, parsed_date, score_details)
            for parent_id, match_id, parsed_date, score_details in unique_matches
            if match_id not in existing_stats
        ]

        # Cap at 500 — 500 × 0.35s = 175s fits within 5 min lock window
        to_process = to_process[:500]

        logger.info(
            f"📊 Detailed stats: "
            f"{len(unique_matches)} unique matches | "
            f"{len(existing_stats)} already done | "
            f"{len(to_process)} to process now"
        )

        processed     = 0
        failed        = 0
        no_fixture    = 0
        no_stats_ever = set()  # past matches confirmed to never have stats
        batch_start   = time.time()

        for parent_id, match_id, parsed_date, score_details in to_process:
            # Safety cap: stop before the 5-min lock expires
            if time.time() - batch_start > 270:
                logger.info("⏱ Batch time limit reached, stopping early")
                break

            try:
                from football.tasks import process_single_fixture_stats

                result = process_single_fixture_stats(
                    match_fixture_id=match_id,
                    parent_fixture_id=parent_id,
                    date=parsed_date,
                    score_details=score_details
                )

                if result.get('status') == 'success':
                    processed += 1

                elif result.get('status') == 'no_fixture':
                    # Historical match not in our Fixture table — expected
                    # These are past matches from form JSON, never ingested
                    no_fixture += 1
                    no_stats_ever.add(match_id)

                elif result.get('status') in ['no_data', 'invalid_data']:
                    # API returned nothing — will never have stats
                    no_stats_ever.add(match_id)
                    failed += 1

                else:
                    failed += 1

            except RateLimitExceeded as exc:
                # Sleep until the window clears, then continue.
                # The old "rate_limited = True / break" caused the entire
                # 5-min batch to abort after just 7 items whenever concurrent
                # tasks (h2h, form, standings) had already filled the 7/sec
                # window. Now we pause briefly and keep going.
                logger.warning(
                    f"⚠️ Rate limit hit match {match_id}, "
                    f"sleeping {exc.wait_time:.2f}s then continuing"
                )
                time.sleep(exc.wait_time)

            except Exception as exc:
                logger.error(
                    f"❌ Detailed stats error match {match_id}: {exc}"
                )
                failed += 1

        # ── Check completion for each parent fixture ───────────────────────
        for ingestion in ingestions:
            try:
                all_needed_ids = set()

                # Form match IDs from JSON arrays
                try:
                    adv = FixtureAdvancedStats.objects.get(
                        fixture=ingestion.fixture
                    )
                    for form in [
                        adv.home_last_5_form,
                        adv.away_last_5_form,
                        adv.home_last_5_home_form,
                        adv.away_last_5_away_form,
                        adv.home_last_5_vs_similar_rank,
                        adv.away_last_5_vs_similar_rank
                    ]:
                        if form:
                            for match in form:
                                if match.get('fixture_id'):
                                    all_needed_ids.add(match['fixture_id'])
                except FixtureAdvancedStats.DoesNotExist:
                    pass

                # H2H past match IDs
                h2h_ids = set(
                    HeadToHeadMatch.objects.filter(
                        fixture=ingestion.fixture
                    ).values_list('past_fixture_id', flat=True)
                )
                all_needed_ids.update(h2h_ids)

                if not all_needed_ids:
                    ingestion.needs_detailed_stats        = False
                    ingestion.detailed_stats_processed_at = timezone.now()
                    ingestion.save(update_fields=[
                        'needs_detailed_stats',
                        'detailed_stats_processed_at'
                    ])
                    ingestion.refresh_from_db()
                    ingestion.check_and_mark_complete()
                    logger.info(
                        f"✅ Fixture {ingestion.fixture_id} — "
                        f"no matches needed, marked done"
                    )
                    continue

                # Only check FixtureStatistics — never query Fixture table
                # Historical past matches are NOT in Fixture table
                # fixture_id PK = past match ID = what we stored stats under
                existing_in_db = set(
                    FixtureStatistics.objects.filter(
                        match_id__in=all_needed_ids
                    ).values_list('match_id', flat=True)
                )

                effectively_done = existing_in_db | (
                    all_needed_ids & no_stats_ever
                )

                if len(effectively_done) >= len(all_needed_ids):
                    ingestion.needs_detailed_stats        = False
                    ingestion.detailed_stats_processed_at = timezone.now()
                    ingestion.save(update_fields=[
                        'needs_detailed_stats',
                        'detailed_stats_processed_at'
                    ])
                    ingestion.refresh_from_db()
                    ingestion.check_and_mark_complete()
                    logger.info(
                        f"✅ Fixture {ingestion.fixture_id} fully complete "
                        f"({len(existing_in_db)} with stats, "
                        f"{len(all_needed_ids & no_stats_ever)} no stats ever, "
                        f"total {len(all_needed_ids)} needed)"
                    )
                else:
                    logger.info(
                        f"⏳ Fixture {ingestion.fixture_id} waiting: "
                        f"{len(effectively_done)}/{len(all_needed_ids)} resolved"
                    )

            except Exception as e:
                logger.error(
                    f"Completion check error {ingestion.fixture_id}: {e}"
                )

        remaining = FixtureIngestion.objects.filter(
            needs_detailed_stats=True
        ).count()

        logger.info(
            f"✅ Detailed stats batch done: "
            f"{processed} processed, "
            f"{no_fixture} skipped (not in DB), "
            f"{failed} failed, "
            f"{remaining} parent fixtures remaining"
        )

        return {
            "status":           "success",
            "processed":        processed,
            "no_fixture":       no_fixture,
            "failed":           failed,
            "total_candidates": len(to_process),
            "remaining":        remaining
        }

    finally:
        cache.delete(lock_key)
        logger.info("🔓 Detailed stats lock released")


# ── DAILY STANDINGS REFRESH ───────────────────────────────────────────────────

@shared_task(bind=True, max_retries=3)
def refresh_today_standings(self):
    """
    Refresh standings for every league that had fixtures yesterday.
    Runs once daily at ~1 AM, after all matches from the previous day are over.
    Mirrors the pattern of ingest_next_day_fixtures.

    Fixtures already exist in the DB before they're played, so no status
    filter is needed — any fixture scheduled for that date means the league
    needs its table refreshed once the day is done.

    Cache: standings_{league_id}_{season} keys are deleted before calling
    the helper so the 24-hour cache TTL never blocks this nightly refresh.
    Crontab: 0 1 * * *  (1 AM daily)
    """
    try:
        yesterday = (timezone.now() - timedelta(days=1)).date()

        league_pairs = list(
            Fixture.objects.filter(date__date=yesterday)
            .select_related('league')
            .values_list('league__id', 'league__season')
            .distinct()
        )

        if not league_pairs:
            logger.info(f"📭 refresh_today_standings: no fixtures on {yesterday}")
            return {"status": "no_work", "date": yesterday.isoformat(), "processed": 0}

        logger.info(
            f"🔄 refresh_today_standings: refreshing {len(league_pairs)} leagues "
            f"for {yesterday}"
        )

        # Delete cache keys so the helper always hits the API fresh
        for league_id, season in league_pairs:
            cache.delete(f"standings_{league_id}_{season}")

        from football.tasks import process_single_league_standings

        processed    = 0
        rate_limited = False

        for league_id, season in league_pairs:
            if rate_limited:
                break

            try:
                result = process_single_league_standings(league_id, season)

                if result.get('status') in ('success', 'cached', 'no_data', 'no_valid_data'):
                    processed += 1

            except RateLimitExceeded as exc:
                logger.warning(
                    f"⚠️ Rate limit hit refreshing standings for league {league_id}"
                )
                rate_limited = True

            except Exception as exc:
                logger.error(
                    f"❌ Standings refresh error league {league_id} "
                    f"season {season}: {exc}"
                )

        logger.info(
            f"✅ refresh_today_standings: {processed}/{len(league_pairs)} leagues "
            f"updated for {yesterday}"
        )

        return {
            "status":    "success",
            "date":      yesterday.isoformat(),
            "processed": processed,
            "total":     len(league_pairs),
        }

    except Exception as exc:
        logger.error(f"refresh_today_standings failed: {exc}", exc_info=True)
        raise self.retry(countdown=300, exc=exc)








            
        



            

