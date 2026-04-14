"""
engine.py — Pure scoring and accumulator-building logic.

No API calls, no DB calls, no Redis. All inputs come from the caller.
All methods wrapped in try/except. Type hints and docstrings throughout.
"""

import logging

from django.conf import settings

logger = logging.getLogger(__name__)


class PredictionEngine:
    """Scores fixtures and assembles accumulator bets."""

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_eligible_fixtures(
        self,
        fixtures: list[dict],
        standings: dict,
        predictions: dict,
        odds: dict,
        mode: str = "small",
    ) -> list[dict]:
        """
        Apply gate conditions and return only fixtures that pass all checks.

        Logs which gate failed at DEBUG level for each rejected fixture.
        """
        eligible: list[dict] = []

        for fixture in fixtures:
            fid = fixture.get("fixture_id")
            try:
                # --- All-modes gates ---
                if not fixture.get("advanced_stats"):
                    logger.debug(f"Gate: no advanced_stats — fixture {fid}")
                    continue
                if not predictions.get(fid):
                    logger.debug(f"Gate: no predictions — fixture {fid}")
                    continue
                if not odds.get(fid):
                    logger.debug(f"Gate: no odds — fixture {fid}")
                    continue

                standing = standings.get(fid, {})
                home_st = standing.get("home", {})
                away_st = standing.get("away", {})
                total_teams = standing.get("total_teams", 0)

                if mode == "small":
                    if home_st.get("matches_played", 0) < 8:
                        logger.debug(f"Gate: home matches_played < 8 — fixture {fid}")
                        continue
                    if away_st.get("matches_played", 0) < 8:
                        logger.debug(f"Gate: away matches_played < 8 — fixture {fid}")
                        continue
                    if total_teams < 6:
                        logger.debug(f"Gate: total_teams < 6 — fixture {fid}")
                        continue

                elif mode == "monster":
                    if home_st.get("matches_played", 0) < 5:
                        logger.debug(f"Gate: home matches_played < 5 (monster) — fixture {fid}")
                        continue
                    if away_st.get("matches_played", 0) < 5:
                        logger.debug(f"Gate: away matches_played < 5 (monster) — fixture {fid}")
                        continue
                    if total_teams < 6:
                        logger.debug(f"Gate: total_teams < 6 (monster) — fixture {fid}")
                        continue
                    if not fixture.get("is_priority"):
                        logger.debug(f"Gate: not priority league (monster) — fixture {fid}")
                        continue

                eligible.append(fixture)

            except Exception:
                logger.exception(f"filter_eligible_fixtures: error on fixture {fid}")
                continue

        logger.info(f"filter_eligible_fixtures(mode={mode}): {len(eligible)}/{len(fixtures)} passed")
        return eligible

    # ------------------------------------------------------------------
    # Helper calculators
    # ------------------------------------------------------------------

    def _calc_form_points(self, wins: int, draws: int) -> int:
        """Return wins×3 + draws×1."""
        return wins * 3 + draws * 1

    def _calc_h2h_win_rate(self, h2h: list[dict], team_name: str) -> float:
        """
        Calculate historical H2H win rate for team_name.

        Returns 0.5 if h2h is empty.
        """
        if not h2h:
            return 0.5
        wins = 0
        for match in h2h:
            home_g = match.get("home_goals")
            away_g = match.get("away_goals")
            if home_g is None or away_g is None:
                continue
            if match.get("home_name") == team_name and home_g > away_g:
                wins += 1
            elif match.get("away_name") == team_name and away_g > home_g:
                wins += 1
        return wins / len(h2h)

    def _calc_home_away_rate(self, standings: dict, side: str) -> float:
        """
        Calculate home win rate (side='home') or away win rate (side='away').

        Uses home_stat or away_stat sub-dict from the standings row.
        Returns 0.0 if no data.
        """
        try:
            stat_key = "home_stat" if side == "home" else "away_stat"
            stat = standings.get(side, {}).get(stat_key, {})
            if not stat:
                return 0.0
            played = stat.get("played", 0) or stat.get("matches_played", 0)
            wins = stat.get("win", 0) or stat.get("wins", 0)
            if played > 0:
                return wins / played
        except Exception:
            logger.debug(f"_calc_home_away_rate failed (side={side})")
        return 0.0

    # ------------------------------------------------------------------
    # Market selection
    # ------------------------------------------------------------------

    def select_market(
        self,
        fixture: dict,
        standings: dict,
        odds: dict,
        sub_score: int,
        avg_goals: float,
        predictions: dict | None = None,
    ) -> dict | None:
        """
        Choose the best betting market for a fixture.

        Evaluates markets in priority order (1X2 → BTTS → Over 2.5 →
        Double Chance → Asian Handicap).  Returns a dict describing the
        selected market, or None if nothing qualifies.
        """
        fid = fixture.get("fixture_id")
        odds_data = odds.get(fid, {}) if isinstance(odds, dict) else odds
        if not odds_data:
            return None

        pred = predictions.get(fid, {}) if predictions else {}
        adv = fixture.get("advanced_stats", {})

        try:
            # Priority 1 — 1X2
            if sub_score >= 65:
                home_odds = odds_data.get("match_winner", {}).get("home", 0)
                away_odds = odds_data.get("match_winner", {}).get("away", 0)
                home_pct = pred.get("home_win_pct", 0)
                away_pct = pred.get("away_win_pct", 0)
                if home_pct >= away_pct and 1.25 <= home_odds <= 2.50:
                    return {
                        "market": "1X2",
                        "pick": "Home Win",
                        "odds": home_odds,
                        "no_double_chance": False,
                    }
                if away_pct > home_pct and 1.25 <= away_odds <= 2.50:
                    return {
                        "market": "1X2",
                        "pick": "Away Win",
                        "odds": away_odds,
                        "no_double_chance": False,
                    }

            # Priority 2 — BTTS Yes
            if (
                adv.get("home_goals_scored_last_5", 0) >= 3
                and adv.get("away_goals_scored_last_5", 0) >= 3
            ):
                btts_odds = odds_data.get("btts", {}).get("yes", 0)
                if btts_odds and 1.30 <= btts_odds <= 3.50:
                    return {
                        "market": "BTTS Yes",
                        "pick": "Both Teams Score",
                        "odds": btts_odds,
                        "no_double_chance": False,
                    }

            # Priority 3 — Over 2.5
            if avg_goals >= 2.5:
                over_odds = odds_data.get("over_under", {}).get("over", 0)
                if over_odds and 1.30 <= over_odds <= 3.50:
                    return {
                        "market": "Over 2.5",
                        "pick": "Over 2.5 Goals",
                        "odds": over_odds,
                        "no_double_chance": False,
                    }

            # Priority 4 — Double Chance (fallback when 1X2 odds not in range)
            if sub_score >= 58:
                home_pct = pred.get("home_win_pct", 0)
                away_pct = pred.get("away_win_pct", 0)
                dc_odds = odds_data.get("double_chance", {})
                if home_pct >= away_pct:
                    dc = dc_odds.get("1X", 0)
                    pick = "1X"
                else:
                    dc = dc_odds.get("X2", 0)
                    pick = "X2"
                if dc and 1.20 <= dc <= 3.00:
                    return {
                        "market": "Double Chance",
                        "pick": pick,
                        "odds": dc,
                        "no_double_chance": True,
                    }

            # Priority 5 — Asian Handicap fallback
            home_rate = self._calc_home_away_rate(standings, "home")
            if home_rate >= 0.70:
                home_odds = odds_data.get("match_winner", {}).get("home", 0)
                if home_odds and 1.30 <= home_odds <= 3.50:
                    return {
                        "market": "Asian Handicap",
                        "pick": "Home -0.5",
                        "odds": home_odds,
                        "no_double_chance": False,
                    }

        except Exception:
            logger.exception(f"select_market failed for fixture {fid}")

        return None

    # ------------------------------------------------------------------
    # Single-fixture scoring
    # ------------------------------------------------------------------

    def score_fixture(
        self,
        fixture: dict,
        standings: dict,
        h2h: list[dict],
        predictions: dict,
        odds: dict,
        mode: str = "small",
    ) -> dict | None:
        """
        Score a single fixture and select the best market.

        Returns the fixture dict enriched with confidence, selected market,
        signal breakdown, etc. — or None if the fixture doesn't qualify.
        """
        fid = fixture.get("fixture_id")
        try:
            pred = predictions.get(fid, {})
            adv = fixture.get("advanced_stats", {})
            standing = standings.get(fid, {})

            home_pct = pred.get("home_win_pct", 0) or 0
            away_pct = pred.get("away_win_pct", 0) or 0
            favored = "home" if home_pct >= away_pct else "away"
            low_data = False

            # Signal 1 — Prediction % (25 pts max)
            if not pred:
                s1 = 6
                low_data = True
            else:
                best_pct = max(home_pct, away_pct)
                if best_pct >= 75:
                    s1 = 25
                elif best_pct >= 65:
                    s1 = 18
                elif best_pct >= 55:
                    s1 = 12
                else:
                    s1 = 6

            # Signal 2 — Form points (20 pts max)
            if favored == "home":
                form_pts = self._calc_form_points(
                    adv.get("home_wins_last_5", 0),
                    adv.get("home_draws_last_5", 0),
                )
            else:
                form_pts = self._calc_form_points(
                    adv.get("away_wins_last_5", 0),
                    adv.get("away_draws_last_5", 0),
                )
            if form_pts >= 13:
                s2 = 20
            elif form_pts >= 10:
                s2 = 14
            elif form_pts >= 7:
                s2 = 9
            else:
                s2 = 4

            # Signal 3 — H2H (15 pts max)
            if h2h:
                team_name = (
                    fixture["home_team_name"]
                    if favored == "home"
                    else fixture["away_team_name"]
                )
                rate = self._calc_h2h_win_rate(h2h, team_name)
                if rate >= 0.67:
                    s3 = 15
                elif rate >= 0.50:
                    s3 = 10
                elif rate >= 0.33:
                    s3 = 6
                else:
                    s3 = 3
            else:
                s3 = 6

            # Signal 4 — Home/Away record (15 pts max)
            ha_rate = self._calc_home_away_rate(standing, favored)
            if ha_rate >= 0.65:
                s4 = 15
            elif ha_rate >= 0.50:
                s4 = 10
            elif ha_rate >= 0.35:
                s4 = 6
            else:
                s4 = 3

            # Signal 5 — Goals trend (15 pts max)
            avg_goals = (
                adv.get("home_goals_scored_last_5", 0)
                + adv.get("away_goals_scored_last_5", 0)
            ) / 5
            if avg_goals >= 3.0:
                s5 = 15
            elif avg_goals >= 2.5:
                s5 = 11
            elif avg_goals >= 2.0:
                s5 = 7
            else:
                s5 = 4

            sub_score = s1 + s2 + s3 + s4 + s5

            market_result = self.select_market(
                fixture, standing, odds, sub_score, avg_goals, predictions
            )
            if market_result is None:
                logger.debug(
                    "score_fixture: no market selected for fixture %s", fid
                )
                return None

            # Signal 6 — Odds value (10 pts max)
            selected_odds = market_result["odds"]
            implied_prob = (1 / selected_odds) * 100 if selected_odds else 0
            gap = sub_score - implied_prob
            if gap >= 15:
                s6 = 10
            elif gap >= 5:
                s6 = 7
            elif gap >= 0:
                s6 = 4
            else:
                s6 = 1

            total = sub_score + s6

            # Mode thresholds
            thresholds = {"small": 62, "10k": 62, "100k": 58, "monster": 58}
            threshold = thresholds.get(mode, 62)
            if total < threshold:
                logger.debug(
                    "score_fixture: total %d < threshold %d for fixture %s",
                    total, threshold, fid,
                )
                return None

            return {
                **fixture,
                "confidence": total,
                "selected_market": market_result["market"],
                "selected_pick": market_result["pick"],
                "selected_odds": selected_odds,
                "no_double_chance": market_result.get("no_double_chance", False),
                "is_whitelisted": fixture.get("is_priority", False),
                "favored_team": favored,
                "low_data": low_data,
                # prediction percentages merged so validator can read them
                "home_win_pct": home_pct,
                "draw_pct": pred.get("draw_pct", 0) or 0,
                "away_win_pct": away_pct,
                "advice": pred.get("advice", ""),
                "signal_breakdown": {
                    "s1": s1,
                    "s2": s2,
                    "s3": s3,
                    "s4": s4,
                    "s5": s5,
                    "s6": s6,
                    "sub_score": sub_score,
                    "total": total,
                    "avg_goals": round(avg_goals, 2),
                },
            }

        except Exception:
            logger.exception(f"score_fixture failed for fixture {fid}")
            return None

    # ------------------------------------------------------------------
    # Batch scoring
    # ------------------------------------------------------------------

    def score_all(
        self,
        fixtures: list[dict],
        standings: dict,
        h2h_data: dict,
        predictions: dict,
        odds: dict,
        mode: str = "small",
    ) -> list[dict]:
        """
        Score every fixture in the list and return sorted results.

        Returns fixtures with confidence scores, sorted descending.
        """
        scored: list[dict] = []
        for fixture in fixtures:
            fid = fixture.get("fixture_id")
            h2h = h2h_data.get(fid, [])
            result = self.score_fixture(
                fixture, standings, h2h, predictions, odds, mode
            )
            if result:
                scored.append(result)

        scored.sort(key=lambda f: f["confidence"], reverse=True)
        logger.info(f"score_all(mode={mode}): {len(scored)} scored")
        return scored

    # ------------------------------------------------------------------
    # Accumulator building
    # ------------------------------------------------------------------

    def build_accas(self, scored: list[dict], mode: str = "small") -> dict:
        """
        Build accumulator bets from scored fixtures.

        mode='small': up to 10 daily 3-5 leg accas targeting 4.50–5.50 odds.
        mode='monster': builds a 10k and a 100k long-term accumulator.
        """
        if mode == "small":
            return self._build_small_accas(scored)
        return self._build_monster_accas(scored)

    # --- Small accas ---

    def _build_small_accas(self, scored: list[dict]) -> dict:
        """Build daily small accumulator bets."""
        try:
            if len(scored) < settings.PREDICTION_MIN_FIXTURES:
                logger.info(
                    "build_accas(small): only %d fixtures, need %d",
                    len(scored), settings.PREDICTION_MIN_FIXTURES,
                )
                return {
                    "daily_accas": [],
                    "best_acca": None,
                    "insufficient_fixtures": True,
                }

            # Pre-filter: small accas require confidence >= 65 and no low_data
            eligible = [
                f for f in scored
                if f.get("confidence", 0) >= 65
                and not f.get("low_data", False)
            ]

            daily_accas: list[dict] = []
            used_ids: set[int] = set()

            for _ in range(10):
                candidates = [
                    f for f in eligible
                    if f["fixture_id"] not in used_ids
                ]
                if not candidates:
                    break

                legs: list[dict] = []
                used_leagues: set[int] = set()
                total_odds = 1.0
                has_priority = False

                for candidate in sorted(
                    candidates,
                    key=lambda f: f["confidence"],
                    reverse=True,
                ):
                    # Skip duplicate leagues
                    if candidate["league_id"] in used_leagues:
                        continue
                    # Odds cap
                    if candidate["selected_odds"] > 3.50:
                        continue
                    projected = total_odds * candidate["selected_odds"]
                    if projected > 5.60:
                        continue

                    legs.append(candidate)
                    used_leagues.add(candidate["league_id"])
                    total_odds *= candidate["selected_odds"]
                    if candidate.get("is_priority"):
                        has_priority = True

                    if len(legs) >= 3 and 4.50 <= total_odds <= 5.50:
                        break
                    if len(legs) >= 5:
                        break

                # Validation gates
                if len(legs) < 3:
                    continue
                if not 4.50 <= total_odds <= 5.50:
                    continue

                # Rule: must have at least 1 priority-league leg
                if not has_priority:
                    logger.debug(
                        "build_accas(small): discarding acca — no priority leg"
                    )
                    continue

                # Rule: minimum avg confidence 68
                avg_conf = sum(l["confidence"] for l in legs) / len(legs)
                if avg_conf < 68:
                    logger.debug(
                        "build_accas(small): discarding acca — avg_conf %.1f < 68",
                        avg_conf,
                    )
                    continue

                # Rule: max odds gap between legs <= 1.50
                leg_odds = [l["selected_odds"] for l in legs]
                if max(leg_odds) - min(leg_odds) > 1.50:
                    logger.debug(
                        "build_accas(small): discarding acca — odds gap %.2f > 1.50",
                        max(leg_odds) - min(leg_odds),
                    )
                    continue

                daily_accas.append({
                    "legs": legs,
                    "total_odds": round(total_odds, 2),
                    "avg_confidence": round(avg_conf, 1),
                    "n_legs": len(legs),
                })
                for leg in legs:
                    used_ids.add(leg["fixture_id"])

            best_acca = None
            if daily_accas:
                best_acca = max(
                    daily_accas,
                    key=lambda a: (
                        a["avg_confidence"],
                        -abs(a["total_odds"] - 5.00),
                    ),
                )

            return {
                "daily_accas": daily_accas,
                "best_acca": best_acca,
                "insufficient_fixtures": False,
            }

        except Exception:
            logger.exception("_build_small_accas failed")
            return {
                "daily_accas": [],
                "best_acca": None,
                "insufficient_fixtures": True,
            }

    # --- Monster accas ---

    def _build_monster_accas(self, scored: list[dict]) -> dict:
        """Build long-range 10k and 100k monster accumulator bets."""
        try:
            if len(scored) < settings.PREDICTION_MIN_MONSTER_FIXTURES:
                logger.info(
                    "build_accas(monster): only %d fixtures, need %d",
                    len(scored), settings.PREDICTION_MIN_MONSTER_FIXTURES,
                )
                return {
                    "acca_10k": None,
                    "acca_100k": None,
                    "insufficient_monster_fixtures": True,
                }

            def build_monster(
                target_min: float,
                target_max: float,
                max_legs: int,
                min_leagues: int,
                min_confidence: int,
                allow_double_chance: bool,
            ) -> dict | None:
                """Inner helper that builds a single monster acca."""
                try:
                    candidates = [
                        f for f in scored
                        if f.get("confidence", 0) >= min_confidence
                        and (
                            allow_double_chance
                            or not f.get("no_double_chance", False)
                        )
                    ]
                    # Prefer Over 2.5 and BTTS first
                    candidates.sort(
                        key=lambda f: (
                            0 if f.get("selected_market") in ("Over 2.5", "BTTS Yes")
                            else 1,
                            -f["confidence"],
                        )
                    )

                    legs: list[dict] = []
                    total_odds = 1.0
                    league_counts: dict[int, int] = {}

                    for candidate in candidates:
                        if total_odds > target_max:
                            break
                        if len(legs) >= max_legs:
                            break
                        lid = candidate["league_id"]
                        if league_counts.get(lid, 0) >= 3:
                            continue
                        c_odds = candidate["selected_odds"]
                        if c_odds >= 2.00:
                            continue
                        total_odds *= c_odds
                        legs.append(candidate)
                        league_counts[lid] = league_counts.get(lid, 0) + 1

                    n_leagues = len({l["league_id"] for l in legs})
                    if not (target_min <= total_odds <= target_max):
                        return None
                    if n_leagues < min_leagues:
                        return None

                    return {
                        "legs": legs,
                        "total_odds": round(total_odds, 2),
                        "avg_confidence": round(
                            sum(l["confidence"] for l in legs) / len(legs), 1
                        ) if legs else 0,
                        "n_legs": len(legs),
                        "n_leagues": n_leagues,
                        "start_date": min(
                            l["kickoff_date_short"] for l in legs
                        ) if legs else "",
                        "end_date": max(
                            l["kickoff_date_short"] for l in legs
                        ) if legs else "",
                    }
                except Exception:
                    logger.exception("build_monster inner failed")
                    return None

            acca_10k = build_monster(
                target_min=8000,
                target_max=12000,
                max_legs=20,
                min_leagues=5,
                min_confidence=62,
                allow_double_chance=False,
            )
            acca_100k = build_monster(
                target_min=80000,
                target_max=120000,
                max_legs=50,
                min_leagues=8,
                min_confidence=58,
                allow_double_chance=False,
            )

            return {
                "acca_10k": acca_10k,
                "acca_100k": acca_100k,
                "insufficient_monster_fixtures": False,
            }

        except Exception:
            logger.exception("_build_monster_accas failed")
            return {
                "acca_10k": None,
                "acca_100k": None,
                "insufficient_monster_fixtures": True,
            }
