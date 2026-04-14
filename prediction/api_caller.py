"""
api_caller.py — External API calls for the prediction pipeline.

Handles API-Football /predictions and /odds endpoints.
All calls are Redis-cached. 1.2 s sleep between real network calls.
No sleep when serving from cache.
"""

import json
import logging
import time
import redis
import requests
from django.conf import settings

logger = logging.getLogger(__name__)

REDIS_CLIENT = redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)

_api_football_key = settings.API_KEY

API_FOOTBALL_HEADERS = {
    'x-apisports-key': _api_football_key
}
API_FOOTBALL_BASE =  settings.BASE_API_URL

# Bookmaker priority order: bet365 (8) → 1xBet (6) → William Hill (16)
_BOOKMAKER_PRIORITY = [8, 11, 7]


class APICaller:
    """Wrapper around API-Football and The Odds API with Redis caching."""

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _cache_get(self, key: str) -> dict | None:
        """Return a cached value from Redis, or None if missing/errored."""
        try:
            raw = REDIS_CLIENT.get(key)
            if raw:
                return json.loads(raw)
        except Exception:
            logger.debug(f"Redis cache_get failed for key: {key}")
        return None

    def _cache_set(self, key: str, value: dict, ttl: int) -> None:
        """Store a value in Redis with an expiry TTL (seconds)."""
        try:
            REDIS_CLIENT.setex(key, ttl, json.dumps(value))
        except Exception:
            logger.debug(f"Redis cache_set failed for key: {key}")

    # ------------------------------------------------------------------
    # Individual fixture calls
    # ------------------------------------------------------------------

    def get_predictions(self, fixture_id: int) -> dict:
        """
        Fetch API-Football /predictions for a single fixture.

        Returns a dict with home_win_pct, draw_pct, away_win_pct,
        advice and winner — or {} on any failure.
        Caches the result in Redis.
        """
        cache_key = f"prediction:cache:prediction:{fixture_id}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            logger.debug(f"get_predictions cache hit for fixture {fixture_id}")
            return cached

        try:
            url = f"{API_FOOTBALL_BASE}/predictions"
            response = requests.get(
                url,
                headers=API_FOOTBALL_HEADERS,
                params={"fixture": fixture_id},
                timeout=15,
            )
            time.sleep(1.2)
            response.raise_for_status()
            data = response.json()

            resp_list = data.get("response", [])
            if not resp_list:
                logger.warning(f"No prediction response for fixture {fixture_id}")
                return {}

            # response[0] has a nested "predictions" key
            outer = resp_list[0]
            pred = outer.get("predictions", {})
            percent = pred.get("percent", {})
            winner_info = pred.get("winner", {}) or {}

            def _pct(val: str | None) -> float:
                if not val:
                    return 0.0
                try:
                    return float(str(val).replace("%", "").strip())
                except (ValueError, TypeError):
                    return 0.0

            result = {
                "home_win_pct": _pct(percent.get("home")),
                "draw_pct": _pct(percent.get("draw")),
                "away_win_pct": _pct(percent.get("away")),
                "advice": str(pred.get("advice") or ""),
                "winner": winner_info.get("name") if winner_info else None,
            }

            self._cache_set(
                cache_key, result,
                settings.PREDICTION_CACHE_TTL_PREDICTIONS,
            )
            return result

        except Exception:
            logger.exception(f"get_predictions failed for fixture {fixture_id}")
            return {}

    def get_odds(self, fixture_id: int) -> dict:
        """
        Fetch API-Football /odds for a single fixture.

        Returns a dict with bookmaker, match_winner, btts, over_under
        and double_chance — or {} on any failure.
        Caches the result in Redis.
        """
        cache_key = f"prediction:cache:odds:{fixture_id}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            logger.debug(f"get_odds cache hit for fixture {fixture_id}")
            return cached

        try:
            url = f"{API_FOOTBALL_BASE}/odds"
            response = requests.get(
                url,
                headers=API_FOOTBALL_HEADERS,
                params={"fixture": fixture_id},
                timeout=15,
            )
            time.sleep(1.2)
            response.raise_for_status()
            data = response.json()

            resp_list = data.get("response", [])
            if not resp_list:
                logger.warning(f"No odds response for fixture {fixture_id}")
                return {}

            bookmakers = resp_list[0].get("bookmakers", [])
            bm_map = {bm["id"]: bm for bm in bookmakers}

            # Pick first available bookmaker by priority
            chosen_bm = None
            for bm_id in _BOOKMAKER_PRIORITY:
                if bm_id in bm_map:
                    chosen_bm = bm_map[bm_id]
                    break

            if chosen_bm is None and bookmakers:
                chosen_bm = bookmakers[0]

            if chosen_bm is None:
                logger.warning(f"No bookmaker data for fixture {fixture_id}")
                return {}

            bets_by_name: dict[str, list[dict]] = {}
            for bet in chosen_bm.get("bets", []):
                bets_by_name[bet["name"]] = bet.get("values", [])

            def _odds_val(values: list[dict], label: str) -> float | None:
                for v in values:
                    if v.get("value") == label:
                        try:
                            return float(v["odd"])
                        except (KeyError, TypeError, ValueError):
                            return None
                return None

            # Match Winner
            mw_values = bets_by_name.get("Match Winner", [])
            match_winner = {
                "home": _odds_val(mw_values, "Home") or 0.0,
                "draw": _odds_val(mw_values, "Draw") or 0.0,
                "away": _odds_val(mw_values, "Away") or 0.0,
            }

            # Both Teams Score
            btts_values = bets_by_name.get("Both Teams Score", [])
            btts = {
                "yes": _odds_val(btts_values, "Yes") or 0.0,
                "no": _odds_val(btts_values, "No") or 0.0,
            }

            # Goals Over/Under — find 2.5 line specifically
            ou_values = bets_by_name.get("Goals Over/Under", [])
            over_2_5: float | None = None
            under_2_5: float | None = None
            for v in ou_values:
                label = str(v.get("value", ""))
                if label == "Over 2.5":
                    try:
                        over_2_5 = float(v["odd"])
                    except (KeyError, ValueError):
                        pass
                elif label == "Under 2.5":
                    try:
                        under_2_5 = float(v["odd"])
                    except (KeyError, ValueError):
                        pass
            over_under = {
                "over": over_2_5 or 0.0,
                "under": under_2_5 or 0.0,
            }

            # Double Chance
            # API returns "Home/Draw", "Draw/Away", "Home/Away" — normalise to 1X/X2/12
            dc_values = bets_by_name.get("Double Chance", [])
            double_chance = {
                "1X": _odds_val(dc_values, "Home/Draw") or 0.0,
                "X2": _odds_val(dc_values, "Draw/Away") or 0.0,
                "12": _odds_val(dc_values, "Home/Away") or 0.0,
            }

            result = {
                "bookmaker": chosen_bm.get("name", ""),
                "match_winner": match_winner,
                "btts": btts,
                "over_under": over_under,
                "double_chance": double_chance,
            }

            self._cache_set(
                cache_key, result,
                settings.PREDICTION_CACHE_TTL_ODDS,
            )
            return result

        except Exception:
            logger.exception(f"get_odds failed for fixture {fixture_id}")
            return {}

    def get_fixture_result(self, fixture_id: int) -> dict | None:
        """
        Fetch the result of a fixture from API-Football /fixtures endpoint.

        Returns:
            {
                "status": str,
                "is_finished": bool,
                "home_goals": int or None,
                "away_goals": int or None,
            }
            or None on any failure.

        Uses a short TTL cache (1 hour) for finished matches only.
        In-progress or NS fixtures are never cached.
        """
        finished_statuses = {"FT", "AET", "PEN", "AWD"}

        # Only cache finished results
        cache_key = f"prediction:cache:result:{fixture_id}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            logger.debug(f"get_fixture_result cache hit for fixture {fixture_id}")
            return cached

        try:
            url = f"{API_FOOTBALL_BASE}/fixtures"
            response = requests.get(
                url,
                headers=API_FOOTBALL_HEADERS,
                params={"id": fixture_id},
                timeout=15,
            )
            time.sleep(1.2)
            response.raise_for_status()
            data = response.json()

            resp_list = data.get("response", [])
            if not resp_list:
                logger.warning(f"No fixture response for id {fixture_id}")
                return None

            fixture_data = resp_list[0]
            status_short = fixture_data.get("fixture", {}).get("status", {}).get("short", "")
            goals = fixture_data.get("goals", {})
            home_goals = goals.get("home")
            away_goals = goals.get("away")

            is_finished = status_short in finished_statuses

            result = {
                "status": status_short,
                "is_finished": is_finished,
                "home_goals": home_goals,
                "away_goals": away_goals,
            }

            # Only cache finished results — no point caching NS/live
            if is_finished:
                self._cache_set(cache_key, result, 3600)

            return result

        except Exception:
            logger.exception(f"get_fixture_result failed for fixture {fixture_id}")
            return None

    # ------------------------------------------------------------------
    # Batch methods
    # ------------------------------------------------------------------

    def get_batch_predictions(self, fixture_ids: list[int]) -> dict[int, dict]:
        """
        Fetch predictions for a list of fixture IDs.

        Respects the 1.2 s sleep only for uncached calls.
        Returns a dict keyed by fixture_id.
        """
        result: dict[int, dict] = {}
        for fixture_id in fixture_ids:
            pred = self.get_predictions(fixture_id)
            result[fixture_id] = pred
        return result

    def get_batch_odds(self, fixture_ids: list[int]) -> dict[int, dict]:
        """
        Fetch odds for a list of fixture IDs.

        Respects the 1.2 s sleep only for uncached calls.
        Returns a dict keyed by fixture_id.
        """
        result: dict[int, dict] = {}
        for fixture_id in fixture_ids:
            odds = self.get_odds(fixture_id)
            result[fixture_id] = odds
        return result
