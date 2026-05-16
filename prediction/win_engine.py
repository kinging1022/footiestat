"""
win_engine.py — "Either Team Wins" (1 or 2 / no-draw) prediction engine.

Finds matches where a draw is unlikely — either team could win.
Bet type: Double Chance "12" (Home/Away), i.e. the match will NOT end in a draw.

Products:
  - Up to 50 individual "1 or 2" picks  (sorted by confidence)
  - 100x   accumulator  (~18 legs, target   80x–200x)
  - 1K     accumulator  (~28 legs, target  800x–2000x)
  - 100K   accumulator  (~40 legs, target 80000x–220000x)
"""

import logging

logger = logging.getLogger(__name__)


class WinEngine:

    MAX_PICKS = 50
    MAX_DC_ODDS = 1.80    # Double Chance "12" upper bound
    MIN_DC_ODDS = 1.20    # Double Chance "12" lower bound
    SCORE_THRESHOLD = 55  # minimum win_score to qualify

    # ------------------------------------------------------------------
    # Single-fixture scoring
    # ------------------------------------------------------------------

    def score_win(
        self,
        fixture: dict,
        standings: dict,
        h2h: list[dict],
        predictions: dict,
        odds: dict,
    ) -> dict | None:
        """
        Score a fixture for "either team wins" (no draw) probability.

        Returns an enriched fixture dict with win_score, win_odds, draw_pct
        and signal breakdown — or None if the fixture does not qualify.
        """
        fid = fixture.get("fixture_id")
        try:
            pred = predictions.get(fid, {}) if predictions else {}
            adv = fixture.get("advanced_stats", {})
            standing = standings.get(fid, {}) if standings else {}
            odds_data = odds.get(fid, {}) if isinstance(odds, dict) else odds

            if not odds_data:
                return None

            dc_12 = odds_data.get("double_chance", {}).get("12", 0)
            draw_odds = odds_data.get("match_winner", {}).get("draw", 0)

            # Must have a "12" Double Chance market in a useful odds range
            if not dc_12 or not (self.MIN_DC_ODDS <= dc_12 <= self.MAX_DC_ODDS):
                return None

            # Draw priced below 2.20 means the market thinks draw is likely — skip
            if draw_odds and draw_odds < 2.20:
                return None

            draw_pct = float(pred.get("draw_pct", 0) or 0)
            home_pct = float(pred.get("home_win_pct", 0) or 0)
            away_pct = float(pred.get("away_win_pct", 0) or 0)
            combined_win_pct = home_pct + away_pct

            # API explicitly says draw is the most likely outcome — skip
            if draw_pct > 0 and draw_pct > home_pct and draw_pct > away_pct:
                logger.debug(
                    "score_win: fixture %s rejected — draw_pct=%.1f dominates "
                    "(home=%.1f away=%.1f)",
                    fid, draw_pct, home_pct, away_pct,
                )
                return None

            # ── Signal 1: API draw probability (30 pts max) ──────────────
            # Lower draw_pct → more likely a decisive result.
            if draw_pct == 0:
                s1 = 14  # no API data — rely on other signals
            elif draw_pct <= 15:
                s1 = 30
            elif draw_pct <= 20:
                s1 = 25
            elif draw_pct <= 25:
                s1 = 18
            elif draw_pct <= 30:
                s1 = 12
            elif draw_pct <= 35:
                s1 = 6
            else:
                s1 = 2

            # ── Signal 2: H2H draw rate (20 pts max) ─────────────────────
            valid_h2h = [
                m for m in (h2h or [])
                if m.get("home_goals") is not None and m.get("away_goals") is not None
            ]
            h2h_draw_rate = 0.0

            if valid_h2h:
                h2h_draws = sum(
                    1 for m in valid_h2h
                    if (m.get("home_goals") or 0) == (m.get("away_goals") or 0)
                )
                h2h_draw_rate = h2h_draws / len(valid_h2h)

                if h2h_draw_rate <= 0.15:
                    s2 = 20
                elif h2h_draw_rate <= 0.25:
                    s2 = 15
                elif h2h_draw_rate <= 0.35:
                    s2 = 10
                elif h2h_draw_rate <= 0.50:
                    s2 = 5
                else:
                    s2 = 1
            else:
                s2 = 8  # neutral — no H2H data

            # Hard guard: H2H is draw-heavy — this fixture tends not to produce a winner
            if valid_h2h and h2h_draw_rate > 0.40:
                logger.debug(
                    "score_win: fixture %s rejected — H2H draw rate %.0f%% > 40%%",
                    fid, h2h_draw_rate * 100,
                )
                return None

            # ── Signal 3: Recent form decisiveness — both teams (25 pts max)
            # Count W/L results (decisive) across both teams' last 5 overall.
            home_form = [
                m for m in adv.get("home_last_5_form", [])
                if isinstance(m, dict) and m.get("opponent") != "No data"
            ]
            away_form = [
                m for m in adv.get("away_last_5_form", [])
                if isinstance(m, dict) and m.get("opponent") != "No data"
            ]
            total_form_matches = len(home_form) + len(away_form)

            if total_form_matches >= 8:
                decisive = sum(
                    1 for m in home_form + away_form
                    if m.get("result") in ("W", "L")
                )
                decisive_rate = decisive / total_form_matches
                # Hard guard: both teams drawing too often in recent games
                if decisive_rate < 0.50:
                    logger.debug(
                        "score_win: fixture %s rejected — form decisive rate %.0f%% < 50%%",
                        fid, decisive_rate * 100,
                    )
                    return None
                if decisive_rate >= 0.80:
                    s3 = 25
                elif decisive_rate >= 0.65:
                    s3 = 18
                else:
                    s3 = 10
            else:
                s3 = 10  # neutral — insufficient form data

            # ── Signal 4: Market draw price (15 pts max) ──────────────────
            # High draw odds = bookmaker thinks draw is unlikely.
            if draw_odds >= 4.50:
                s4 = 15
            elif draw_odds >= 3.50:
                s4 = 11
            elif draw_odds >= 3.00:
                s4 = 8
            elif draw_odds >= 2.50:
                s4 = 5
            elif draw_odds >= 2.00:
                s4 = 2
            else:
                s4 = 6  # no draw odds data — neutral

            # ── Signal 5: Combined season win rate from standings (10 pts max)
            home_stat = standing.get("home", {}).get("home_stat", {})
            away_stat = standing.get("away", {}).get("away_stat", {})
            home_played = home_stat.get("played", 0) or 0
            home_wins = home_stat.get("win", 0) or home_stat.get("wins", 0) or 0
            away_played = away_stat.get("played", 0) or 0
            away_wins = away_stat.get("win", 0) or away_stat.get("wins", 0) or 0

            if home_played >= 3 and away_played >= 3:
                avg_win_rate = (home_wins / home_played + away_wins / away_played) / 2
                if avg_win_rate >= 0.55:
                    s5 = 10
                elif avg_win_rate >= 0.40:
                    s5 = 7
                elif avg_win_rate >= 0.25:
                    s5 = 4
                else:
                    s5 = 1
            else:
                s5 = 5  # neutral

            win_score = s1 + s2 + s3 + s4 + s5

            if win_score < self.SCORE_THRESHOLD:
                logger.debug(
                    "score_win: fixture %s scored %d — below threshold %d",
                    fid, win_score, self.SCORE_THRESHOLD,
                )
                return None

            return {
                **fixture,
                "win_score": win_score,
                "win_odds": dc_12,
                "win_pct": combined_win_pct,
                "draw_pct": draw_pct,
                "win_side": "12",
                "win_team": "Either Team Wins",
                "h2h_draw_rate": round(h2h_draw_rate, 2) if valid_h2h else None,
                "signal_breakdown": {
                    "s1_api_draw_pct": s1,
                    "s2_h2h_draw_rate": s2,
                    "s3_form_decisive": s3,
                    "s4_market_draw_price": s4,
                    "s5_season_win_rate": s5,
                    "total": win_score,
                },
            }

        except Exception:
            logger.exception("score_win failed for fixture %s", fid)
            return None

    # ------------------------------------------------------------------
    # Batch scoring
    # ------------------------------------------------------------------

    def score_all_wins(
        self,
        fixtures: list[dict],
        standings: dict,
        h2h_data: dict,
        predictions: dict,
        odds: dict,
    ) -> list[dict]:
        """
        Score every fixture for "either team wins" (no draw) probability.

        Returns fixtures sorted by win_score descending, capped at MAX_PICKS.
        """
        scored: list[dict] = []
        for fixture in fixtures:
            fid = fixture.get("fixture_id")
            h2h = h2h_data.get(fid, [])
            result = self.score_win(fixture, standings, h2h, predictions, odds)
            if result:
                scored.append(result)

        scored.sort(key=lambda f: f["win_score"], reverse=True)
        picks = scored[: self.MAX_PICKS]
        logger.info(
            "score_all_wins: %d/%d fixtures qualified, capped at %d picks",
            len(scored), len(fixtures), len(picks),
        )
        return picks

    # ------------------------------------------------------------------
    # Accumulator building
    # ------------------------------------------------------------------

    def build_win_accas(self, scored_wins: list[dict]) -> dict:
        """
        Build 100x, 1K, and 100K–200K win accumulators from scored picks.

        All legs use Double Chance "12" odds (MIN_DC_ODDS–MAX_DC_ODDS).

        100x   : ~18 legs, target    80x–200x
        1K     : ~28 legs, target   800x–2000x
        100K   : ~40 legs, target 80000x–220000x
        """
        try:
            acca_100 = self._build_win_acca(
                scored_wins,
                target_min=80,
                target_max=200,
                min_legs=14,
                max_legs=24,
                label="100x",
            )
            acca_1k = self._build_win_acca(
                scored_wins,
                target_min=800,
                target_max=2_000,
                min_legs=22,
                max_legs=34,
                label="1k",
            )
            acca_100k = self._build_win_acca(
                scored_wins,
                target_min=80_000,
                target_max=220_000,
                min_legs=36,
                max_legs=self.MAX_PICKS,
                label="100k",
            )
            available = [a for a in [acca_100, acca_1k, acca_100k] if a]
            logger.info(
                "build_win_accas: built %d/3 (100x=%s 1k=%s 100k=%s) from %d picks",
                len(available),
                "yes" if acca_100 else "no",
                "yes" if acca_1k else "no",
                "yes" if acca_100k else "no",
                len(scored_wins),
            )
            return {
                "acca_100": acca_100,
                "acca_1k": acca_1k,
                "acca_100k": acca_100k,
                "insufficient": not available,
            }
        except Exception:
            logger.exception("build_win_accas failed")
            return {
                "acca_100": None,
                "acca_1k": None,
                "acca_100k": None,
                "insufficient": True,
            }

    def _build_win_acca(
        self,
        scored_wins: list[dict],
        target_min: float,
        target_max: float,
        min_legs: int,
        max_legs: int,
        label: str = "",
    ) -> dict | None:
        """
        Build one win accumulator to the given target-odds range.

        Selection order: score DESC, then odds DESC.
        Max 3 legs from the same league.
        Allows a 10% overshoot buffer on target_max.
        """
        tag = f"_build_win_acca({label})"
        try:
            eligible = [
                f for f in scored_wins
                if self.MIN_DC_ODDS <= f.get("win_odds", 0) <= self.MAX_DC_ODDS
            ]
            eligible.sort(key=lambda f: (-f["win_score"], -f["win_odds"]))

            legs: list[dict] = []
            total_odds = 1.0
            league_counts: dict[int, int] = {}

            for candidate in eligible:
                if total_odds >= target_max:
                    break
                if len(legs) >= max_legs:
                    break
                lid = candidate.get("league_id")
                if league_counts.get(lid, 0) >= 3:
                    continue
                projected = total_odds * candidate["win_odds"]
                if projected > target_max * 1.10:
                    continue
                legs.append(candidate)
                total_odds *= candidate["win_odds"]
                if lid:
                    league_counts[lid] = league_counts.get(lid, 0) + 1

            if len(legs) < min_legs:
                logger.info("%s: FAIL — only %d legs (need %d)", tag, len(legs), min_legs)
                return None
            if total_odds < target_min:
                logger.info(
                    "%s: FAIL — total_odds=%.0f < target_min=%.0f (legs=%d)",
                    tag, total_odds, target_min, len(legs),
                )
                return None

            n_leagues = len({l.get("league_id") for l in legs})

            raw_dates = [leg.get("date") for leg in legs if leg.get("date")]
            try:
                sorted_dates = sorted(raw_dates)
                d0, d1 = sorted_dates[0], sorted_dates[-1]
                start_date = (
                    d0.strftime("%a %d %b") if hasattr(d0, "strftime") else str(d0)[:10]
                )
                end_date = (
                    d1.strftime("%a %d %b") if hasattr(d1, "strftime") else str(d1)[:10]
                )
            except Exception:
                start_date = end_date = ""

            logger.info(
                "%s: OK — legs=%d n_leagues=%d total_odds=%.0f",
                tag, len(legs), n_leagues, total_odds,
            )
            return {
                "legs": legs,
                "total_odds": round(total_odds, 2),
                "avg_score": round(
                    sum(l["win_score"] for l in legs) / len(legs), 1
                ),
                "n_legs": len(legs),
                "n_leagues": n_leagues,
                "start_date": start_date,
                "end_date": end_date,
                "label": label.upper(),
            }
        except Exception:
            logger.exception("%s inner failed", tag)
            return None
