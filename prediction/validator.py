"""
validator.py — Claude AI validation of scored fixtures.

Each fixture is sent to Claude for a conservative betting-analyst review.
Results are cached in Redis. Defaults to REJECT on any error.
"""

import json
import logging
import time

import anthropic
import redis
from django.conf import settings

logger = logging.getLogger(__name__)

REDIS_CLIENT = redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)


class ClaudeValidator:
    """Uses Claude to validate whether a scored betting pick has genuine value."""

    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    MODEL = "claude-sonnet-4-6"
    MAX_TOKENS = 1000

    def _cache_key(self, fixture_id: int) -> str:
        """Return the Redis cache key for a fixture's Claude verdict."""
        return f"prediction:cache:claude:{fixture_id}"

    def validate_fixture(self, fixture: dict) -> dict:
        """
        Validate a single scored fixture using Claude.

        Returns the fixture dict merged with verdict, claude_reason and
        adjusted_confidence fields.  Defaults to REJECT on any error.
        """
        fixture_id = fixture.get("fixture_id")
        cache_key = self._cache_key(fixture_id)

        try:
            cached_raw = REDIS_CLIENT.get(cache_key)
            if cached_raw:
                cached = json.loads(cached_raw)
                logger.debug(f"Claude cache hit for fixture {fixture_id}")
                return {
                    **fixture,
                    "verdict": cached.get("verdict", "REJECT"),
                    "claude_reason": cached.get("claude_reason", ""),
                    "adjusted_confidence": cached.get(
                        "adjusted_confidence", fixture.get("confidence", 0)
                    ),
                }
        except Exception:
            logger.debug(f"Claude cache read failed for fixture {fixture_id}")

        adv = fixture.get("advanced_stats", {})
        h2h = fixture.get("h2h", [])
        sb = fixture.get("signal_breakdown", {})

        h2h_lines = "\n".join(
            f"  {m.get('home_name')} {m.get('home_goals', '?')}-"
            f"{m.get('away_goals', '?')} {m.get('away_name')} ({m.get('date', '')})"
            for m in (h2h if isinstance(h2h, list) else [])
        ) or "  No H2H data available"

        prompt = f"""Match: {fixture.get('home_team_name')} vs {fixture.get('away_team_name')}
League: {fixture.get('league_name')} ({fixture.get('country_name')})
Kickoff: {fixture.get('kickoff_str')}
---
Predictions:
  Home Win: {fixture.get('home_win_pct', 'N/A')}%
  Draw: {fixture.get('draw_pct', 'N/A')}%
  Away Win: {fixture.get('away_win_pct', 'N/A')}%
  API Advice: {fixture.get('advice', 'N/A')}
---
Home team last 5 overall:
  {adv.get('home_wins_last_5', 0)}W {adv.get('home_draws_last_5', 0)}D {adv.get('home_losses_last_5', 0)}L
  Goals scored: {adv.get('home_goals_scored_last_5', 0)}
  Goals conceded: {adv.get('home_goals_conceded_last_5', 0)}
Home team last 5 at home:
  {adv.get('home_home_wins_last_5', 0)}W {adv.get('home_home_draws_last_5', 0)}D {adv.get('home_home_losses_last_5', 0)}L
---
Away team last 5 overall:
  {adv.get('away_wins_last_5', 0)}W {adv.get('away_draws_last_5', 0)}D {adv.get('away_losses_last_5', 0)}L
  Goals scored: {adv.get('away_goals_scored_last_5', 0)}
  Goals conceded: {adv.get('away_goals_conceded_last_5', 0)}
Away team last 5 away:
  {adv.get('away_away_wins_last_5', 0)}W {adv.get('away_away_draws_last_5', 0)}D {adv.get('away_away_losses_last_5', 0)}L
---
H2H last 6:
{h2h_lines}
---
Selected pick:
  Market: {fixture.get('selected_market')}
  Pick: {fixture.get('selected_pick')}
  Odds: {fixture.get('selected_odds')}
Rules engine confidence: {fixture.get('confidence')}/100
Signal breakdown: {sb}"""

        system_prompt = (
            "You are a strict professional football betting analyst. "
            "Review the fixture data and decide if the betting pick has genuine value. "
            "Be conservative. "
            "Consider what stats cannot show: derbies, dead rubbers, title clinched, "
            "relegation confirmed, cup fatigue, rest rotation, tactical setups, "
            "manager tendencies. "
            "Respond ONLY in valid JSON, no extra text:\n"
            '{"verdict": "APPROVE" or "REJECT" or "DOWNGRADE", '
            '"reason": "one sentence maximum", '
            '"adjusted_confidence": integer}\n'
            "APPROVE: solid pick, any acca.\n"
            "DOWNGRADE: borderline, small accas only.\n"
            "REJECT: exclude from all accas.\n"
            "adjusted_confidence must never exceed input."
        )

        verdict = "REJECT"
        reason = "validation error"
        adj_conf = fixture.get("confidence", 0)

        try:
            time.sleep(1.2)
            message = self.client.messages.create(
                model=self.MODEL,
                max_tokens=self.MAX_TOKENS,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_text = message.content[0].text.strip()
            parsed = json.loads(raw_text)
            verdict = parsed.get("verdict", "REJECT")
            reason = parsed.get("reason", "")
            adj_conf = int(parsed.get("adjusted_confidence", adj_conf))
            # Claude must never exceed the input confidence
            adj_conf = min(adj_conf, fixture.get("confidence", 0))
            if verdict not in ("APPROVE", "REJECT", "DOWNGRADE"):
                verdict = "REJECT"

        except (json.JSONDecodeError, KeyError, IndexError, ValueError) as exc:
            logger.warning(
                "Claude response parse error for fixture %s: %s", fixture_id, exc
            )
            verdict = "REJECT"
            reason = "validation error"
            adj_conf = fixture.get("confidence", 0)
        except Exception:
            logger.exception(f"Claude API call failed for fixture {fixture_id}")
            verdict = "REJECT"
            reason = "validation error"
            adj_conf = fixture.get("confidence", 0)

        # Cache the verdict
        try:
            REDIS_CLIENT.setex(
                cache_key,
                settings.PREDICTION_CACHE_TTL_CLAUDE,
                json.dumps({
                    "verdict": verdict,
                    "claude_reason": reason,
                    "adjusted_confidence": adj_conf,
                }),
            )
        except Exception:
            logger.debug(f"Failed to cache Claude verdict for fixture {fixture_id}")

        return {
            **fixture,
            "verdict": verdict,
            "claude_reason": reason,
            "adjusted_confidence": adj_conf,
        }

    def validate_batch(self, fixtures: list[dict]) -> list[dict]:
        """
        Validate a list of scored fixtures through Claude.

        Returns only APPROVE and DOWNGRADE fixtures (REJECT is excluded).
        Logs a summary of verdicts at the end.
        """
        approved: list[dict] = []
        counts = {"APPROVE": 0, "DOWNGRADE": 0, "REJECT": 0}

        for fixture in fixtures:
            result = self.validate_fixture(fixture)
            v = result.get("verdict", "REJECT")
            counts[v] = counts.get(v, 0) + 1
            if v != "REJECT":
                approved.append(result)

        logger.info(
            "validate_batch: APPROVE=%d DOWNGRADE=%d REJECT=%d",
            counts["APPROVE"], counts["DOWNGRADE"], counts["REJECT"],
        )
        return approved
