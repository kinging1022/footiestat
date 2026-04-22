"""
db_reader.py — Django ORM queries for the prediction pipeline.

"""

import logging
from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from football.models import (
    Fixture,
    FixtureAdvancedStats,
    HeadToHeadMatch,
    LeagueTableSnapshot,
)

logger = logging.getLogger(__name__)

BLACKLIST_COUNTRIES = {
    "Faroe Islands", "San Marino", "Andorra",
    "Gibraltar", "Liechtenstein", "Kosovo", "Malta",
    "Luxembourg", "Armenia", "Azerbaijan", "Tajikistan",
    "Kyrgyzstan", "Turkmenistan", "Mongolia", "Bhutan",
    "Maldives", "Macau", "Bangladesh", "Cambodia",
    "Laos", "Myanmar", "Papua New Guinea", "Fiji",
    "Vanuatu", "Samoa",
}

BLACKLIST_ROUND_KEYWORDS = [
    "Playoff", "Relegation", "Promotion", "Final",
    "Semi-final", "Quarter-final", "Play-off",
]

RESERVE_KEYWORDS = [
    " II", " B ", " B)", "Res.", "Reserve",
    "Reserves", " U21", " U23", " U20", " U19",
    " U18", "Youth", "Junior", "Juniors",
    "Academy", "Filial", "Sub-", "sub ",
]


def is_reserve_team(name: str) -> bool:
    """Return True if team name suggests a reserve, youth or B team."""
    name_upper = name.upper()
    for kw in RESERVE_KEYWORDS:
        if kw.upper() in name_upper:
            return True
    return False


class DBReader:
    """Reads football data from the Django ORM for the prediction pipeline."""

    def get_todays_fixtures(self, mode: str = "small") -> list[dict]:
        """
        Return upcoming fixtures serialised as dicts.

        mode='small': fixtures in the next 24 hours.
        mode='monster': fixtures within PREDICTION_MONSTER_DAYS_AHEAD days,
                        limited to priority leagues.
        """
        try:
            now = timezone.now()

            # Small mode: require full processing (all 5 stages done).
            # Monster mode: only require advanced_stats to be computed —
            # detailed_stats (stage 3) is never used by the prediction pipeline
            # and is the slowest stage, so fixtures 3-7 days out often have all
            # the data we need but are still blocked on it.
            if mode == "monster":
                ingestion_filter = {"ingestion__needs_advanced_stats": False}
            else:
                ingestion_filter = {"ingestion__is_fully_processed": True}

            qs = Fixture.objects.filter(
                date__gte=now + timedelta(hours=1),
                status=Fixture.STATUS_NS,
                **ingestion_filter,
            ).select_related(
                "league",
                "league__country",
                "home_team",
                "away_team",
                "advanced_stats",
            )

            if mode == "small":
                qs = qs.filter(date__lte=now + timedelta(hours=24))
            elif mode == "monster":
                qs = qs.filter(
                    date__lte=now + timedelta(
                        days=settings.PREDICTION_MONSTER_DAYS_AHEAD
                    ),
                    league__priority__lte=settings.PREDICTION_PRIORITY_THRESHOLD,
                )

            results: list[dict] = []
            for fixture in qs:
                # Monster pipeline: enforce round and reserve filters so only
                # clean, regular-season priority fixtures are included.
                # Daily pipeline (small): cast a wide net — Claude handles quality.
                if mode == "monster":
                    if fixture.round:
                        round_lower = fixture.round.lower()
                        if any(kw.lower() in round_lower for kw in BLACKLIST_ROUND_KEYWORDS):
                            logger.debug(
                                f"Skipping fixture {fixture.id} — blacklisted round: {fixture.round}"
                            )
                            continue
                    if is_reserve_team(fixture.home_team.name) or is_reserve_team(fixture.away_team.name):
                        logger.debug(
                            f"Skipped reserve team fixture: "
                            f"{fixture.home_team.name} vs {fixture.away_team.name}"
                        )
                        continue

                if mode == "small":
                    country_name = fixture.league.country.name
                    if country_name in BLACKLIST_COUNTRIES:
                        logger.debug(
                            f"Skipping fixture {fixture.id} — blacklisted country: {country_name}"
                        )
                        continue
                    if is_reserve_team(fixture.home_team.name) or is_reserve_team(fixture.away_team.name):
                        logger.debug(
                            f"Skipped reserve/youth team fixture (small): "
                            f"{fixture.home_team.name} vs {fixture.away_team.name}"
                        )
                        continue

                adv = getattr(fixture, "advanced_stats", None)
                if adv is None:
                    logger.debug(
                        f"Skipping fixture {fixture.id} — no advanced_stats"
                    )
                    continue

                results.append(self._serialize_fixture(fixture, adv))

            # Sort: priority first, then by league priority ASC
            results.sort(key=lambda f: (0 if f["is_priority"] else 1, f["league_priority"]))

            logger.info(f"get_todays_fixtures(mode={mode}) → {len(results)} fixtures")
            return results

        except Exception:
            logger.exception(f"get_todays_fixtures failed (mode={mode})")
            return []

    def _serialize_fixture(self, fixture: Fixture, adv: FixtureAdvancedStats) -> dict:
        """Serialise a Fixture + FixtureAdvancedStats pair to a plain dict."""
        return {
            "fixture_id": fixture.id,
            "date": fixture.date,
            "kickoff_str": fixture.date.strftime("%a %d %b %Y %H:%M UTC"),
            "kickoff_date_short": fixture.date.strftime("%a %d %b"),
            "league_id": fixture.league.id,
            "league_name": fixture.league.name,
            "league_priority": fixture.league.priority,
            "league_season": fixture.league.season,
            "country_name": fixture.league.country.name,
            "is_priority": fixture.league.priority <= settings.PREDICTION_PRIORITY_THRESHOLD,
            "round": fixture.round,
            "home_team_id": fixture.home_team.id,
            "home_team_name": fixture.home_team.name,
            "away_team_id": fixture.away_team.id,
            "away_team_name": fixture.away_team.name,
            "advanced_stats": {
                "home_wins_last_5": int(adv.home_wins_last_5),
                "home_draws_last_5": int(adv.home_draws_last_5),
                "home_losses_last_5": int(adv.home_losses_last_5),
                "home_goals_scored_last_5": int(adv.home_goals_scored_last_5),
                "home_goals_conceded_last_5": int(adv.home_goals_conceded_last_5),
                "home_home_wins_last_5": int(adv.home_home_wins_last_5),
                "home_home_draws_last_5": int(adv.home_home_draws_last_5),
                "home_home_losses_last_5": int(adv.home_home_losses_last_5),
                "away_wins_last_5": int(adv.away_wins_last_5),
                "away_draws_last_5": int(adv.away_draws_last_5),
                "away_losses_last_5": int(adv.away_losses_last_5),
                "away_goals_scored_last_5": int(adv.away_goals_scored_last_5),
                "away_goals_conceded_last_5": int(adv.away_goals_conceded_last_5),
                "away_away_wins_last_5": int(adv.away_away_wins_last_5),
                "away_away_draws_last_5": int(adv.away_away_draws_last_5),
                "away_away_losses_last_5": int(adv.away_away_losses_last_5),
                "home_last_5_form": list(adv.home_last_5_form),
                "away_last_5_form": list(adv.away_last_5_form),
                "home_last_5_home_form": list(adv.home_last_5_home_form),
                "away_last_5_away_form": list(adv.away_last_5_away_form),
                "home_last_5_vs_similar_rank": list(adv.home_last_5_vs_similar_rank or []),
                "away_last_5_vs_similar_rank": list(adv.away_last_5_vs_similar_rank or []),
            },
        }

    def get_standings(
        self,
        league_id: int,
        home_team_id: int,
        away_team_id: int,
        season: int,
    ) -> dict:
        """
        Return standings rows for both teams in a fixture.

        Returns a dict with 'home', 'away' and 'total_teams' keys,
        or {} on failure.
        """
        try:
            snapshots = LeagueTableSnapshot.objects.filter(
                league_id=league_id,
                season=season,
                team_id__in=[home_team_id, away_team_id],
            )

            total_teams = LeagueTableSnapshot.objects.filter(
                league_id=league_id,
                season=season,
            ).count()

            snap_map: dict[int, LeagueTableSnapshot] = {
                s.team_id: s for s in snapshots
            }

            def _row(snap: LeagueTableSnapshot | None) -> dict:
                if snap is None:
                    return {
                        "matches_played": 0,
                        "wins": 0,
                        "draws": 0,
                        "losses": 0,
                        "home_stat": {},
                        "away_stat": {},
                        "last_five": "",
                        "rank": 0,
                        "points": 0,
                    }
                return {
                    "matches_played": snap.matches_played,
                    "wins": snap.wins,
                    "draws": snap.draws,
                    "losses": snap.losses,
                    "home_stat": snap.home_stat or {},
                    "away_stat": snap.away_stat or {},
                    "last_five": snap.last_five or "",
                    "rank": snap.rank,
                    "points": snap.points,
                }

            return {
                "home": _row(snap_map.get(home_team_id)),
                "away": _row(snap_map.get(away_team_id)),
                "total_teams": total_teams,
            }

        except Exception:
            logger.exception(
                "get_standings failed (league=%s, season=%s)", league_id, season
            )
            return {}

    def get_h2h(self, fixture_id: int) -> list[dict]:
        """
        Return the last 6 head-to-head matches for a fixture.

        Returns a list of dicts, or [] on failure.
        """
        try:
            matches = HeadToHeadMatch.objects.filter(
                fixture_id=fixture_id
            ).order_by("-date")[:6]

            return [
                {
                    "home_name": m.home_name,
                    "away_name": m.away_name,
                    "date": m.date.strftime("%Y-%m-%d"),
                    "home_goals": m.home_fulltime_goals,
                    "away_goals": m.away_fulltime_goals,
                }
                for m in matches
            ]

        except Exception:
            logger.exception(f"get_h2h failed (fixture_id={fixture_id})")
            return []

    

    def get_batch_standings(self, fixtures: list[dict]) -> dict:
        """
        Run get_standings for each fixture in the list.

        Returns a dict keyed by fixture_id.
        """
        result: dict[int, dict] = {}
        for f in fixtures:
            result[f["fixture_id"]] = self.get_standings(
                league_id=f["league_id"],
                home_team_id=f["home_team_id"],
                away_team_id=f["away_team_id"],
                season=f["league_season"],
            )
        return result

    def get_batch_h2h(self, fixtures: list[dict]) -> dict:
        """
        Run get_h2h for each fixture in the list.

        Returns a dict keyed by fixture_id.
        """
        result: dict[int, list[dict]] = {}
        for f in fixtures:
            result[f["fixture_id"]] = self.get_h2h(f["fixture_id"])
        return result
