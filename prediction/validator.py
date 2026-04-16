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
                result = {
                    **fixture,
                    "verdict": cached.get("verdict", "REJECT"),
                    "claude_reason": cached.get("claude_reason", ""),
                    "adjusted_confidence": cached.get(
                        "adjusted_confidence", fixture.get("confidence", 0)
                    ),
                }
                if cached.get("selected_market"):
                    result["selected_market"] = cached["selected_market"]
                    result["selected_pick"] = cached["selected_pick"]
                    result["selected_odds"] = cached["selected_odds"]
                    result["no_double_chance"] = cached.get("no_double_chance", False)
                return result
        except Exception:
            logger.debug(f"Claude cache read failed for fixture {fixture_id}")

        adv = fixture.get("advanced_stats", {})
        h2h = fixture.get("h2h", [])
        sb = fixture.get("signal_breakdown", {})
        market_options = fixture.get("market_options", [])

        h2h_lines = "\n".join(
            f"  {m.get('home_name')} {m.get('home_goals', '?')}-"
            f"{m.get('away_goals', '?')} {m.get('away_name')} ({m.get('date', '')})"
            for m in (h2h if isinstance(h2h, list) else [])
        ) or "  No H2H data available"

        markets_text = "\n".join(
            f"  {i+1}. {m['market']} | {m['pick']} | Odds: {m['odds']}"
            for i, m in enumerate(market_options)
        ) or f"  1. {fixture.get('selected_market')} | {fixture.get('selected_pick')} | Odds: {fixture.get('selected_odds')}"

        def _sum_similar(matches: list, key: str) -> int:
            return sum(
                m.get(key, 0) for m in (matches or [])
                if isinstance(m, dict) and m.get("opponent") != "No data"
            )

        home_sim = adv.get("home_last_5_vs_similar_rank", [])
        away_sim = adv.get("away_last_5_vs_similar_rank", [])
        home_sim_n = sum(1 for m in home_sim if isinstance(m, dict) and m.get("opponent") != "No data")
        away_sim_n = sum(1 for m in away_sim if isinstance(m, dict) and m.get("opponent") != "No data")

        similar_block = ""
        if home_sim_n >= 2 or away_sim_n >= 2:
            similar_block = f"""
vs similar-rank opponents (last {home_sim_n} / {away_sim_n} matches):
  Home: {_sum_similar(home_sim, 'goals_scored')} scored, {_sum_similar(home_sim, 'goals_conceded')} conceded
  Away: {_sum_similar(away_sim, 'goals_scored')} scored, {_sum_similar(away_sim, 'goals_conceded')} conceded"""

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
  {adv.get('away_away_wins_last_5', 0)}W {adv.get('away_away_draws_last_5', 0)}D {adv.get('away_away_losses_last_5', 0)}L{similar_block}
---
H2H last 6:
{h2h_lines}
---
Available markets (you must select one or REJECT all):
{markets_text}
Rules engine confidence: {fixture.get('confidence')}/100
Signal breakdown: {sb}"""

        system_prompt = (
            "You are a professional football betting analyst. "
            "You are given a fixture and a list of available betting markets that have passed a rules engine. "
            "Your job is to: (1) select the single best market for this fixture, or REJECT if there is a clear red flag. "
            "Red flags: dead rubber, title already clinched, relegation confirmed, derby chaos, cup fatigue, known injury crisis, or obvious tactical mismatch. "
            "If no red flag exists, pick the market with the best value given the data. "
            "Prefer 1X2 when there is a clear favourite. Prefer Over 2.5 when both teams score freely. "
            "Prefer BTTS only when both defenses are genuinely open. "
            "Prefer Double Chance when the favourite is clear but the margin is tight. "
            "Do not REJECT based on low odds or general uncertainty. "
            "Respond ONLY in valid JSON, no extra text:\n"
            '{"verdict": "APPROVE" or "REJECT" or "DOWNGRADE", '
            '"reason": "MUST be one short sentence. Maximum 12 words. No stats, no numbers, no odds values, no percentages. '
            'Just the key reason in plain English. '
            "Examples: 'Strong home form against a weak away side.' | "
            "'Both teams have scored freely in last 5.' | "
            "'Away team dominant, home defence leaking.' | "
            "'Dominant form and clear market value.' "
            'Violating the 12 word limit is not permitted.", '
            '"adjusted_confidence": integer, '
            '"selected_market": "market name", '
            '"selected_pick": "pick name"}\n'
            "APPROVE: solid pick, include in any acca.\n"
            "DOWNGRADE: minor concern, small accas only.\n"
            "REJECT: clear red flag, exclude from all accas.\n"
            "adjusted_confidence must never exceed input. "
            "selected_market and selected_pick must match one of the available markets exactly."
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
            # Strip markdown code fences if Claude wraps the JSON
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]
                raw_text = raw_text.strip()
            parsed = json.loads(raw_text)
            verdict = parsed.get("verdict", "REJECT")
            reason = parsed.get("reason", "")
            # Safety net: hard-truncate reason to 12 words
            words = reason.split()
            if len(words) > 12:
                reason = " ".join(words[:12]) + "."
            adj_conf = int(parsed.get("adjusted_confidence", adj_conf))
            # Claude must never exceed the input confidence
            adj_conf = min(adj_conf, fixture.get("confidence", 0))
            if verdict not in ("APPROVE", "REJECT", "DOWNGRADE"):
                verdict = "REJECT"

            # Apply Claude's market selection if provided and valid
            claude_market = parsed.get("selected_market", "")
            claude_pick = parsed.get("selected_pick", "")
            if claude_market and claude_pick:
                # Verify it matches one of the available options
                options = fixture.get("market_options", [])
                match = next(
                    (m for m in options if m["market"] == claude_market),
                    None,
                )
                if match:
                    fixture = {
                        **fixture,
                        "selected_market": match["market"],
                        "selected_pick": match["pick"],
                        "selected_odds": match["odds"],
                        "no_double_chance": match.get("no_double_chance", False),
                    }

        except (json.JSONDecodeError, KeyError, IndexError, ValueError) as exc:
            logger.warning(
                f"Claude response parse error for fixture {fixture_id}: {exc} | raw={raw_text!r}"
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
                    "selected_market": fixture.get("selected_market"),
                    "selected_pick": fixture.get("selected_pick"),
                    "selected_odds": fixture.get("selected_odds"),
                    "no_double_chance": fixture.get("no_double_chance", False),
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
