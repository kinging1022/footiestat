"""
draw_engine.py — Draw prediction scoring and pick selection.

Identifies matches with high draw probability using 6 independent signals.
Produces daily (value) and longshot (higher odds) draw picks targeting
5–13 total selections per day.
"""

import logging

logger = logging.getLogger(__name__)


class DrawEngine:
    """Scores fixtures for draw probability and assembles draw pick lists."""

    DAILY_SCORE_THRESHOLD = 52
    LONGSHOT_SCORE_THRESHOLD = 38
    DAILY_ODDS_MIN = 2.50
    DAILY_ODDS_MAX = 4.80
    LONGSHOT_ODDS_MIN = 3.60
    LONGSHOT_ODDS_MAX = 7.00
    MAX_DAILY_PICKS = 8
    MAX_LONGSHOT_PICKS = 5

    # ------------------------------------------------------------------
    # Single-fixture draw scoring
    # ------------------------------------------------------------------

    def score_draw(
        self,
        fixture: dict,
        standings: dict,
        h2h: list[dict],
        predictions: dict,
        odds: dict,
    ) -> dict | None:
        """
        Score a single fixture for draw probability.

        Returns an enriched fixture dict with draw_score, draw_odds and
        signal breakdown — or None if the fixture scores below the longshot
        threshold or has no valid draw odds.
        """
        fid = fixture.get("fixture_id")
        try:
            pred = predictions.get(fid, {}) if predictions else {}
            adv = fixture.get("advanced_stats", {})
            standing = standings.get(fid, {}) if standings else {}
            odds_data = odds.get(fid, {}) if isinstance(odds, dict) else odds

            if not odds_data:
                return None

            draw_odds = odds_data.get("match_winner", {}).get("draw", 0)
            home_odds = odds_data.get("match_winner", {}).get("home", 0)
            away_odds = odds_data.get("match_winner", {}).get("away", 0)

            # Gate: draw odds must exist and sit in a realistic range
            if not draw_odds or draw_odds < 2.20 or draw_odds > 7.00:
                logger.debug(
                    "score_draw: fixture %s rejected — draw_odds=%.2f out of range",
                    fid, draw_odds or 0,
                )
                return None

            # ── Signal 1: API Draw Prediction % (25 pts max) ─────────────
            # The API model's own draw probability is the single strongest signal.
            draw_pct = float(pred.get("draw_pct", 0) or 0)
            if draw_pct >= 35:
                s1 = 25
            elif draw_pct >= 28:
                s1 = 19
            elif draw_pct >= 22:
                s1 = 13
            elif draw_pct >= 16:
                s1 = 8
            else:
                s1 = 3

            # ── Signal 2: H2H Draw Rate (20 pts max) ─────────────────────
            # Historical draw patterns in this specific rivalry are sticky.
            valid_h2h = [
                m for m in (h2h or [])
                if m.get("home_goals") is not None and m.get("away_goals") is not None
            ]
            h2h_draws = 0
            h2h_draw_rate_val: float | None = None
            if valid_h2h:
                h2h_draws = sum(
                    1 for m in valid_h2h
                    if m.get("home_goals") == m.get("away_goals")
                )
                h2h_draw_rate_val = round(h2h_draws / len(valid_h2h), 2)
                if h2h_draw_rate_val >= 0.45:
                    s2 = 20
                elif h2h_draw_rate_val >= 0.33:
                    s2 = 14
                elif h2h_draw_rate_val >= 0.22:
                    s2 = 8
                else:
                    s2 = 3
            else:
                s2 = 8  # neutral — no H2H data available

            # ── Signal 3: Odds Evenness (15 pts max) ─────────────────────
            # When the market prices home and away very similarly, neither team
            # has a clear edge — draws become the default outcome.
            if home_odds > 0 and away_odds > 0:
                ratio = min(home_odds, away_odds) / max(home_odds, away_odds)
                if ratio >= 0.82:
                    s3 = 15
                elif ratio >= 0.70:
                    s3 = 10
                elif ratio >= 0.58:
                    s3 = 5
                else:
                    s3 = 2
            else:
                s3 = 5  # neutral

            # ── Signal 4: Draw Odds Zone (15 pts max) ────────────────────
            # 2.70–3.80 is the market sweet spot — these are the draws the
            # bookmaker considers genuinely probable. Above 4.80 they become
            # deep longshots; below 2.50 the market already prices the draw in
            # heavily and the edge is gone.
            if 2.70 <= draw_odds <= 3.80:
                s4 = 15
            elif 2.50 <= draw_odds < 2.70:
                s4 = 11
            elif 3.81 <= draw_odds <= 4.80:
                s4 = 10
            elif 4.81 <= draw_odds <= 6.50:
                s4 = 6
            else:
                s4 = 2

            # ── Signal 5: Recent Draw Form — both teams (15 pts max) ─────
            # Teams currently in a draw pattern carry that momentum forward.
            # Use the higher of overall or venue-specific draw counts for
            # robustness when one dataset is sparse.
            home_draws = int(adv.get("home_draws_last_5", 0) or 0)
            away_draws = int(adv.get("away_draws_last_5", 0) or 0)
            home_home_draws = int(adv.get("home_home_draws_last_5", 0) or 0)
            away_away_draws = int(adv.get("away_away_draws_last_5", 0) or 0)
            combined_overall = home_draws + away_draws
            combined_venue = home_home_draws + away_away_draws
            combined_recent = max(combined_overall, combined_venue)
            if combined_recent >= 5:
                s5 = 15
            elif combined_recent >= 4:
                s5 = 11
            elif combined_recent >= 3:
                s5 = 7
            elif combined_recent >= 2:
                s5 = 4
            else:
                s5 = 1

            # ── Signal 6: Season Draw Rate (10 pts max) ──────────────────
            # Some teams structurally draw a lot all season. When both sides
            # carry a high season-long draw rate the fixture is inherently
            # draw-prone regardless of form.
            home_st = standing.get("home", {})
            away_st = standing.get("away", {})
            home_mp = int(home_st.get("matches_played", 0) or 0)
            away_mp = int(away_st.get("matches_played", 0) or 0)
            home_szn_draws = int(home_st.get("draws", 0) or 0)
            away_szn_draws = int(away_st.get("draws", 0) or 0)
            if home_mp >= 5 and away_mp >= 5:
                avg_szn_draw_rate = (
                    (home_szn_draws / home_mp) + (away_szn_draws / away_mp)
                ) / 2
                if avg_szn_draw_rate >= 0.35:
                    s6 = 10
                elif avg_szn_draw_rate >= 0.28:
                    s6 = 7
                elif avg_szn_draw_rate >= 0.22:
                    s6 = 4
                else:
                    s6 = 1
            else:
                s6 = 4  # neutral — insufficient season data

            draw_score = s1 + s2 + s3 + s4 + s5 + s6

            if draw_score < self.LONGSHOT_SCORE_THRESHOLD:
                logger.debug(
                    "score_draw: fixture %s scored %d — below longshot threshold %d",
                    fid, draw_score, self.LONGSHOT_SCORE_THRESHOLD,
                )
                return None

            return {
                **fixture,
                "draw_score": draw_score,
                "draw_odds": draw_odds,
                "draw_pct": draw_pct,
                "h2h_draw_rate": h2h_draw_rate_val,
                "combined_recent_draws": combined_recent,
                "signal_breakdown": {
                    "s1_api_draw_pct": s1,
                    "s2_h2h_draw_rate": s2,
                    "s3_odds_evenness": s3,
                    "s4_draw_odds_zone": s4,
                    "s5_recent_form": s5,
                    "s6_season_rate": s6,
                    "total": draw_score,
                },
            }

        except Exception:
            logger.exception("score_draw failed for fixture %s", fid)
            return None

    # ------------------------------------------------------------------
    # Batch scoring
    # ------------------------------------------------------------------

    def score_all_draws(
        self,
        fixtures: list[dict],
        standings: dict,
        h2h_data: dict,
        predictions: dict,
        odds: dict,
    ) -> list[dict]:
        """
        Score every fixture for draw probability.

        Returns fixtures sorted by draw_score descending.
        """
        scored: list[dict] = []
        for fixture in fixtures:
            fid = fixture.get("fixture_id")
            h2h = h2h_data.get(fid, [])
            result = self.score_draw(fixture, standings, h2h, predictions, odds)
            if result:
                scored.append(result)

        scored.sort(key=lambda f: f["draw_score"], reverse=True)
        logger.info("score_all_draws: %d/%d fixtures qualified", len(scored), len(fixtures))
        return scored

    # ------------------------------------------------------------------
    # Pick selection
    # ------------------------------------------------------------------

    def build_draw_picks(self, scored_draws: list[dict]) -> dict:
        """
        Split scored draws into daily (solid value) and longshot buckets.

        Daily  : score >= 52, draw odds 2.50–4.80 → up to 8 picks.
        Longshot: score >= 38, draw odds 3.60–7.00 → up to 5 picks,
                  never includes a fixture already in the daily list.

        Returns {daily, longshot, total, insufficient}.
        insufficient=True when fewer than 5 total picks were found.
        """
        try:
            daily_ids: set[int] = set()
            daily: list[dict] = []

            for pick in scored_draws:
                if len(daily) >= self.MAX_DAILY_PICKS:
                    break
                if pick["draw_score"] < self.DAILY_SCORE_THRESHOLD:
                    continue
                if not (self.DAILY_ODDS_MIN <= pick["draw_odds"] <= self.DAILY_ODDS_MAX):
                    continue
                daily.append(pick)
                daily_ids.add(pick["fixture_id"])

            longshot: list[dict] = []
            for pick in scored_draws:
                if len(longshot) >= self.MAX_LONGSHOT_PICKS:
                    break
                if pick["fixture_id"] in daily_ids:
                    continue
                if pick["draw_score"] < self.LONGSHOT_SCORE_THRESHOLD:
                    continue
                if not (self.LONGSHOT_ODDS_MIN <= pick["draw_odds"] <= self.LONGSHOT_ODDS_MAX):
                    continue
                longshot.append(pick)

            total = len(daily) + len(longshot)
            logger.info(
                "build_draw_picks: daily=%d longshot=%d total=%d",
                len(daily), len(longshot), total,
            )
            return {
                "daily": daily,
                "longshot": longshot,
                "total": total,
                "insufficient": total < 5,
            }

        except Exception:
            logger.exception("build_draw_picks failed")
            return {"daily": [], "longshot": [], "total": 0, "insufficient": True}

    # ------------------------------------------------------------------
    # Draw accumulators
    # ------------------------------------------------------------------

    def build_draw_accas(self, scored_draws: list[dict]) -> dict:
        """
        Build two draw accumulator bets from scored draws.

        Short draw acca:
          - 2–3 legs from high-confidence daily draws (score >= 52)
          - draw odds 2.50–4.00 per leg (tight, realistic range)
          - target total odds 8x–30x

        Long draw acca:
          - 3–4 legs using the best available draws (score >= 38)
          - draw odds 3.20–7.00 per leg (wider, includes longshots)
          - target total odds 50x–300x

        Returns {short_acca, long_acca}.  Either may be None if the pool
        does not have enough qualifying fixtures.
        """
        try:
            short_acca = self._build_short_draw_acca(scored_draws)
            long_acca = self._build_long_draw_acca(scored_draws)
            logger.info(
                "build_draw_accas: short=%s long=%s",
                "built" if short_acca else "none",
                "built" if long_acca else "none",
            )
            return {"short_acca": short_acca, "long_acca": long_acca}
        except Exception:
            logger.exception("build_draw_accas failed")
            return {"short_acca": None, "long_acca": None}

    def _build_short_draw_acca(self, scored_draws: list[dict]) -> dict | None:
        """
        2–3 leg draw acca targeting 8x–30x total odds.

        Uses the top daily-quality draws (score >= 52, odds 2.50–4.00).
        Sorted by score DESC so the most confident picks anchor the acca.
        """
        try:
            eligible = [
                f for f in scored_draws
                if f["draw_score"] >= self.DAILY_SCORE_THRESHOLD
                and 2.50 <= f["draw_odds"] <= 4.00
            ]
            eligible.sort(key=lambda f: -f["draw_score"])

            legs: list[dict] = []
            total_odds = 1.0
            used_leagues: set[int] = set()

            for candidate in eligible:
                if len(legs) >= 3:
                    break
                lid = candidate.get("league_id")
                if lid in used_leagues:
                    continue
                projected = total_odds * candidate["draw_odds"]
                if projected > 32.0:
                    continue
                legs.append(candidate)
                total_odds *= candidate["draw_odds"]
                if lid:
                    used_leagues.add(lid)

            if len(legs) < 2:
                logger.info("_build_short_draw_acca: only %d legs — need 2", len(legs))
                return None
            if total_odds < 8.0:
                logger.info(
                    "_build_short_draw_acca: total_odds=%.2f < 8.0 minimum", total_odds
                )
                return None

            return {
                "legs": legs,
                "total_odds": round(total_odds, 2),
                "avg_score": round(sum(l["draw_score"] for l in legs) / len(legs), 1),
                "n_legs": len(legs),
                "label": "SHORT",
            }
        except Exception:
            logger.exception("_build_short_draw_acca inner failed")
            return None

    def _build_long_draw_acca(self, scored_draws: list[dict]) -> dict | None:
        """
        3–4 leg draw acca targeting 50x–300x total odds.

        Draws from the full qualifying pool (score >= 38, odds 3.20–7.00).
        Sorted by score DESC first, then odds DESC to compound faster.
        """
        try:
            eligible = [
                f for f in scored_draws
                if f["draw_score"] >= self.LONGSHOT_SCORE_THRESHOLD
                and 3.20 <= f["draw_odds"] <= 7.00
            ]
            eligible.sort(key=lambda f: (-f["draw_score"], -f["draw_odds"]))

            legs: list[dict] = []
            total_odds = 1.0
            used_leagues: set[int] = set()

            for candidate in eligible:
                if len(legs) >= 4:
                    break
                lid = candidate.get("league_id")
                if lid in used_leagues:
                    continue
                projected = total_odds * candidate["draw_odds"]
                if projected > 320.0:
                    continue
                legs.append(candidate)
                total_odds *= candidate["draw_odds"]
                if lid:
                    used_leagues.add(lid)

            if len(legs) < 3:
                logger.info("_build_long_draw_acca: only %d legs — need 3", len(legs))
                return None
            if total_odds < 50.0:
                logger.info(
                    "_build_long_draw_acca: total_odds=%.2f < 50.0 minimum", total_odds
                )
                return None

            return {
                "legs": legs,
                "total_odds": round(total_odds, 2),
                "avg_score": round(sum(l["draw_score"] for l in legs) / len(legs), 1),
                "n_legs": len(legs),
                "label": "LONG",
            }
        except Exception:
            logger.exception("_build_long_draw_acca inner failed")
            return None

    # ------------------------------------------------------------------
    # Draw monster accumulators (1K / 10K / 100K)
    # ------------------------------------------------------------------

    def build_draw_monster_accas(self, scored_draws: list[dict]) -> dict:
        """
        Build 1K, 10K, and 100K draw monster accumulators from a wider
        (7-day) fixture pool.

        1K  monster: 5–6 legs,  target   800x–5,000x
        10K monster: 7–9 legs,  target 8,000x–50,000x
        100K monster: 10–14 legs, target 80,000x–500,000x

        Draw odds per leg: 3.00–7.00.  Each target is built from the
        same pool independently, so their leg selections naturally differ.
        Returns {acca_1k, acca_10k, acca_100k} — any may be None.
        """
        try:
            acca_1k   = self._build_draw_monster(
                scored_draws,
                target_min=800,      target_max=6_000,
                min_legs=5, max_legs=6,
                min_score=self.LONGSHOT_SCORE_THRESHOLD,
                label="1k",
            )
            acca_10k  = self._build_draw_monster(
                scored_draws,
                target_min=8_000,    target_max=60_000,
                min_legs=7, max_legs=9,
                min_score=self.LONGSHOT_SCORE_THRESHOLD,
                label="10k",
            )
            acca_100k = self._build_draw_monster(
                scored_draws,
                target_min=80_000,   target_max=2_000_000,
                min_legs=9, max_legs=14,
                min_score=self.LONGSHOT_SCORE_THRESHOLD,
                label="100k",
            )
            available = [a for a in [acca_1k, acca_10k, acca_100k] if a]
            logger.info(
                "build_draw_monster_accas: built %d/3 "
                "(1k=%s 10k=%s 100k=%s) from %d fixtures",
                len(available),
                "yes" if acca_1k   else "no",
                "yes" if acca_10k  else "no",
                "yes" if acca_100k else "no",
                len(scored_draws),
            )
            return {
                "acca_1k":   acca_1k,
                "acca_10k":  acca_10k,
                "acca_100k": acca_100k,
                "insufficient": not available,
            }
        except Exception:
            logger.exception("build_draw_monster_accas failed")
            return {"acca_1k": None, "acca_10k": None, "acca_100k": None, "insufficient": True}

    def _build_draw_monster(
        self,
        scored_draws: list[dict],
        target_min: float,
        target_max: float,
        min_legs: int,
        max_legs: int,
        min_score: int,
        label: str = "",
    ) -> dict | None:
        """
        Inner helper that builds one draw monster acca to a target-odds range.

        Selection order: score DESC, then odds DESC so high-value legs
        compound the product faster and reach the target with fewer legs.
        Max 2 legs from the same league to ensure diversity.
        Allows a 10% overshoot buffer on target_max.
        """
        tag = f"_build_draw_monster({label})"
        try:
            eligible = [
                f for f in scored_draws
                if f["draw_score"] >= min_score
                and 3.00 <= f["draw_odds"] <= 7.00
            ]
            eligible.sort(key=lambda f: (-f["draw_score"], -f["draw_odds"]))

            legs: list[dict] = []
            total_odds = 1.0
            league_counts: dict[int, int] = {}

            for candidate in eligible:
                if total_odds >= target_max:
                    break
                if len(legs) >= max_legs:
                    break
                lid = candidate.get("league_id")
                if league_counts.get(lid, 0) >= 2:
                    continue
                projected = total_odds * candidate["draw_odds"]
                if projected > target_max * 1.10:
                    continue
                legs.append(candidate)
                total_odds *= candidate["draw_odds"]
                if lid:
                    league_counts[lid] = league_counts.get(lid, 0) + 1

            if len(legs) < min_legs:
                logger.info(
                    "%s: FAIL — only %d legs (need %d)", tag, len(legs), min_legs
                )
                return None
            if total_odds < target_min:
                logger.info(
                    "%s: FAIL — total_odds=%.0f < target_min=%.0f (legs=%d)",
                    tag, total_odds, target_min, len(legs),
                )
                return None

            n_leagues = len({l.get("league_id") for l in legs})

            # Date span — sort legs by kickoff for display
            raw_dates = [leg.get("date") for leg in legs if leg.get("date")]
            try:
                sorted_dates = sorted(raw_dates)
                d0, d1 = sorted_dates[0], sorted_dates[-1]
                start_date = d0.strftime("%a %d %b") if hasattr(d0, "strftime") else str(d0)[:10]
                end_date   = d1.strftime("%a %d %b") if hasattr(d1, "strftime") else str(d1)[:10]
            except Exception:
                start_date = end_date = ""

            logger.info(
                "%s: OK — legs=%d n_leagues=%d total_odds=%.0f",
                tag, len(legs), n_leagues, total_odds,
            )
            return {
                "legs": legs,
                "total_odds": round(total_odds, 2),
                "avg_score": round(sum(l["draw_score"] for l in legs) / len(legs), 1),
                "n_legs": len(legs),
                "n_leagues": n_leagues,
                "start_date": start_date,
                "end_date": end_date,
                "label": label.upper(),
            }
        except Exception:
            logger.exception("%s inner failed", tag)
            return None
