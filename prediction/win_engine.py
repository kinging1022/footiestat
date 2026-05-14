"""
win_engine.py — Heavy-favourite home/away win prediction engine.

Only considers fixtures where one side is priced at ≤ 1.30 (heavy favourite).
Strictly validates that the match ends in a clean team win — no draws included.

Products:
  - Up to 50 individual win picks  (sorted by confidence)
  - 100x   accumulator  (~18 legs, target   80x–200x)
  - 1K     accumulator  (~28 legs, target  800x–2000x)
  - 100K   accumulator  (~40 legs, target 80000x–220000x)
"""

import logging

logger = logging.getLogger(__name__)


class WinEngine:

    MAX_PICKS = 50
    MAX_ODDS = 1.30
    MIN_ODDS = 1.05
    SCORE_THRESHOLD = 60    # minimum win_score to qualify as a pick
    MIN_WIN_PCT = 50         # API prediction floor for the favoured side

    # ------------------------------------------------------------------
    # Single-fixture win scoring
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
        Score a single fixture for heavy-favourite win probability.

        Returns an enriched fixture dict with win_score, win_odds, win_side and
        signal breakdown — or None if the fixture does not qualify.
        """
        fid = fixture.get("fixture_id")
        try:
            pred = predictions.get(fid, {}) if predictions else {}
            adv = fixture.get("advanced_stats", {})
            standing = standings.get(fid, {}) if standings else {}
            odds_data = odds.get(fid, {}) if isinstance(odds, dict) else odds

            if not odds_data:
                return None

            home_odds = odds_data.get("match_winner", {}).get("home", 0)
            away_odds = odds_data.get("match_winner", {}).get("away", 0)

            home_qualifies = bool(home_odds and self.MIN_ODDS <= home_odds <= self.MAX_ODDS)
            away_qualifies = bool(away_odds and self.MIN_ODDS <= away_odds <= self.MAX_ODDS)

            if not home_qualifies and not away_qualifies:
                return None

            # When both qualify (rare), take the shorter odds — stronger favourite
            if home_qualifies and away_qualifies:
                side = "home" if home_odds <= away_odds else "away"
            elif home_qualifies:
                side = "home"
            else:
                side = "away"

            pick_odds = home_odds if side == "home" else away_odds
            home_pct = float(pred.get("home_win_pct", 0) or 0)
            away_pct = float(pred.get("away_win_pct", 0) or 0)
            win_pct = home_pct if side == "home" else away_pct
            team_name = (
                fixture.get("home_team_name", "") if side == "home"
                else fixture.get("away_team_name", "")
            )

            # API win probability hard floor
            if win_pct < self.MIN_WIN_PCT:
                logger.debug(
                    "score_win: fixture %s rejected — %s win_pct=%.1f < %d",
                    fid, side, win_pct, self.MIN_WIN_PCT,
                )
                return None

            # ── Signal 1: API Win Probability (30 pts max) ───────────────
            if win_pct >= 75:
                s1 = 30
            elif win_pct >= 65:
                s1 = 24
            elif win_pct >= 58:
                s1 = 18
            elif win_pct >= 50:
                s1 = 12
            else:
                s1 = 6

            # ── Signal 2: Venue-Context Recent Form (25 pts max) ─────────
            # Home pick → home team's last 5 at home.
            # Away pick → away team's last 5 away.
            if side == "home":
                ctx_form = [
                    m for m in adv.get("home_last_5_home_form", [])
                    if isinstance(m, dict) and m.get("opponent") != "No data"
                ]
            else:
                ctx_form = [
                    m for m in adv.get("away_last_5_away_form", [])
                    if isinstance(m, dict) and m.get("opponent") != "No data"
                ]

            n_ctx = len(ctx_form)
            ctx_wins = sum(1 for m in ctx_form if m.get("result") == "W")

            if n_ctx >= 4:
                ctx_rate = ctx_wins / n_ctx
                if ctx_rate >= 0.80:
                    s2 = 25
                elif ctx_rate >= 0.60:
                    s2 = 19
                elif ctx_rate >= 0.40:
                    s2 = 12
                else:
                    s2 = 4
            elif n_ctx >= 2:
                s2 = 10
            else:
                s2 = 6

            # ── Signal 3: H2H Win Rate (20 pts max) ──────────────────────
            valid_h2h = [
                m for m in (h2h or [])
                if m.get("home_goals") is not None and m.get("away_goals") is not None
            ]
            h2h_win_rate = 0.0
            h2h_draw_rate = 0.0

            if valid_h2h:
                h2h_wins = 0
                h2h_draws = 0
                for m in valid_h2h:
                    hg = m.get("home_goals") or 0
                    ag = m.get("away_goals") or 0
                    if hg == ag:
                        h2h_draws += 1
                    elif m.get("home_name") == team_name and hg > ag:
                        h2h_wins += 1
                    elif m.get("away_name") == team_name and ag > hg:
                        h2h_wins += 1
                n = len(valid_h2h)
                h2h_win_rate = h2h_wins / n
                h2h_draw_rate = h2h_draws / n

                if h2h_win_rate >= 0.70:
                    s3 = 20
                elif h2h_win_rate >= 0.55:
                    s3 = 15
                elif h2h_win_rate >= 0.40:
                    s3 = 10
                elif h2h_win_rate >= 0.25:
                    s3 = 5
                else:
                    s3 = 2
            else:
                s3 = 8  # neutral — no H2H data

            # ── Signal 4: Season Venue Win Rate from Standings (15 pts max)
            side_st = standing.get("home", {}) if side == "home" else standing.get("away", {})
            stat_key = "home_stat" if side == "home" else "away_stat"
            venue_stat = side_st.get(stat_key, {})
            venue_played = (
                venue_stat.get("played", 0)
                or venue_stat.get("matches_played", 0)
                or 0
            )
            venue_wins = venue_stat.get("win", 0) or venue_stat.get("wins", 0) or 0
            venue_win_rate: float | None = (
                venue_wins / venue_played if venue_played >= 3 else None
            )

            if venue_win_rate is not None:
                if venue_win_rate >= 0.70:
                    s4 = 15
                elif venue_win_rate >= 0.55:
                    s4 = 11
                elif venue_win_rate >= 0.40:
                    s4 = 7
                else:
                    s4 = 3
            else:
                s4 = 6  # neutral

            # ── Signal 5: Opponent Weakness in Their Travelling Context (10 pts max)
            opp_side = "away" if side == "home" else "home"
            if opp_side == "away":
                opp_ctx_form = [
                    m for m in adv.get("away_last_5_away_form", [])
                    if isinstance(m, dict) and m.get("opponent") != "No data"
                ]
            else:
                opp_ctx_form = [
                    m for m in adv.get("home_last_5_home_form", [])
                    if isinstance(m, dict) and m.get("opponent") != "No data"
                ]

            n_opp = len(opp_ctx_form)
            opp_losses = sum(1 for m in opp_ctx_form if m.get("result") == "L")

            if n_opp >= 4:
                opp_loss_rate = opp_losses / n_opp
                if opp_loss_rate >= 0.60:
                    s5 = 10
                elif opp_loss_rate >= 0.40:
                    s5 = 7
                elif opp_loss_rate >= 0.20:
                    s5 = 4
                else:
                    s5 = 1
            else:
                s5 = 4  # neutral

            win_score = s1 + s2 + s3 + s4 + s5

            if win_score < self.SCORE_THRESHOLD:
                logger.debug(
                    "score_win: fixture %s scored %d — below threshold %d",
                    fid, win_score, self.SCORE_THRESHOLD,
                )
                return None

            # ── Hard guards ───────────────────────────────────────────────

            # Guard 1: lost last 3 consecutive H2H → no confidence backing a win
            if len(valid_h2h) >= 3:
                lost_last_3 = all(
                    (
                        m.get("home_name") == team_name
                        and (m.get("home_goals") or 0) < (m.get("away_goals") or 0)
                    ) or (
                        m.get("away_name") == team_name
                        and (m.get("away_goals") or 0) < (m.get("home_goals") or 0)
                    )
                    for m in valid_h2h[:3]
                )
                if lost_last_3:
                    logger.debug(
                        "score_win: fixture %s — %s lost last 3 H2H, rejecting",
                        fid, team_name,
                    )
                    return None

            # Guard 2: draw-heavy H2H → this fixture tends not to produce a winner
            if len(valid_h2h) >= 4 and h2h_draw_rate > 0.50:
                logger.debug(
                    "score_win: fixture %s — H2H draw rate %.0f%% > 50%%, rejecting",
                    fid, h2h_draw_rate * 100,
                )
                return None

            # Guard 3: two consecutive venue-context losses → form collapse
            if n_ctx >= 2:
                last_2 = [m.get("result") for m in ctx_form[:2]]
                if last_2 == ["L", "L"]:
                    logger.debug(
                        "score_win: fixture %s — %s lost last 2 venue matches, rejecting",
                        fid, team_name,
                    )
                    return None

            return {
                **fixture,
                "win_score": win_score,
                "win_odds": pick_odds,
                "win_pct": win_pct,
                "win_side": side,
                "win_team": team_name,
                "h2h_win_rate": round(h2h_win_rate, 2) if valid_h2h else None,
                "h2h_draw_rate": round(h2h_draw_rate, 2) if valid_h2h else None,
                "venue_win_rate": round(venue_win_rate, 2) if venue_win_rate is not None else None,
                "signal_breakdown": {
                    "s1_api_win_pct": s1,
                    "s2_venue_form": s2,
                    "s3_h2h": s3,
                    "s4_season_venue": s4,
                    "s5_opp_weakness": s5,
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
        Score every fixture for heavy-favourite win probability.

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

        All legs must have odds ≤ MAX_ODDS (1.30).

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

        Selection order: score DESC, then odds DESC so higher-value legs
        compound the product faster.  Max 3 legs from the same league.
        Allows a 10 % overshoot buffer on target_max.
        """
        tag = f"_build_win_acca({label})"
        try:
            eligible = [
                f for f in scored_wins
                if self.MIN_ODDS <= f.get("win_odds", 0) <= self.MAX_ODDS
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
