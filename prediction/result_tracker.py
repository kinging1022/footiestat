"""
result_tracker.py — Redis-backed win/loss tracking for accumulator products.

No JSON files. No filesystem. All records persist in Redis with no TTL.
All operations are wrapped in try/except.
"""

import json
import logging
from datetime import datetime

import redis
from django.conf import settings

logger = logging.getLogger(__name__)

REDIS_CLIENT = redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)

KEYS = {
    "small_accas":   "prediction:record:small_accas",
    "best_acca":     "prediction:record:best_acca",
    "acca_10k":      "prediction:record:acca_10k",
    "acca_100k":     "prediction:record:acca_100k",
    "acca_daily_100": "prediction:record:acca_daily_100",
    "acca_daily_500": "prediction:record:acca_daily_500",
    "acca_daily_1k":  "prediction:record:acca_daily_1k",
    "pending":       "prediction:pending",
}


class ResultTracker:
    """Tracks accumulator results and win/loss records in Redis."""

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_record(self, product: str) -> dict:
        """Return the win/loss record for a product, or a zeroed default."""
        try:
            raw = REDIS_CLIENT.get(KEYS[product])
            if raw:
                return json.loads(raw)
        except Exception:
            logger.debug(f"_get_record failed for product {product}")
        return {"wins": 0, "losses": 0, "last_10": []}

    def _save_record(self, product: str, record: dict) -> None:
        """Persist a record to Redis with no TTL (permanent)."""
        try:
            REDIS_CLIENT.set(KEYS[product], json.dumps(record))
        except Exception:
            logger.exception(f"_save_record failed for product {product}")

    def _get_pending(self) -> list[dict]:
        """Return the list of pending (unsettled) accas."""
        try:
            raw = REDIS_CLIENT.get(KEYS["pending"])
            if raw:
                return json.loads(raw)
        except Exception:
            logger.debug("_get_pending failed")
        return []

    def _save_pending(self, pending: list[dict]) -> None:
        """Persist the pending accas list to Redis."""
        try:
            REDIS_CLIENT.set(KEYS["pending"], json.dumps(pending))
        except Exception:
            logger.exception("_save_pending failed")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save_accas(self, accas: dict) -> None:
        """
        Save newly generated accas to the pending list.

        Accepts the dict returned by PredictionEngine.build_accas for
        both small and monster modes.
        """
        try:
            pending = self._get_pending()
            now = datetime.utcnow().isoformat()

            product_map: dict[str, list[dict]] = {
                "small_accas":    accas.get("daily_accas") or [],
                "best_acca":      [accas["best_acca"]]  if accas.get("best_acca")  else [],
                "acca_10k":       [accas["acca_10k"]]   if accas.get("acca_10k")   else [],
                "acca_100k":      [accas["acca_100k"]]  if accas.get("acca_100k")  else [],
                "acca_daily_100": [accas["acca_100"]]   if accas.get("acca_100")   else [],
                "acca_daily_500": [accas["acca_500"]]   if accas.get("acca_500")   else [],
                "acca_daily_1k":  [accas["acca_1k"]]    if accas.get("acca_1k")    else [],
            }

            for product, acca_list in product_map.items():
                for acca in acca_list:
                    pending.append({
                        "product": product,
                        "generated_at": now,
                        "total_odds": acca.get("total_odds", 0),
                        "legs": [
                            {
                                "fixture_id": leg["fixture_id"],
                                "home": leg.get("home_team_name", ""),
                                "away": leg.get("away_team_name", ""),
                                "market": leg.get("selected_market", ""),
                                "pick": leg.get("selected_pick", ""),
                                "odds": leg.get("selected_odds", 0),
                                "status": "pending",
                            }
                            for leg in acca.get("legs", [])
                        ],
                        "acca_status": "pending",
                    })

            self._save_pending(pending)
            logger.info(f"save_accas: saved {sum(len(v) for v in product_map.values())} new acca entries to pending")

        except Exception:
            logger.exception("save_accas failed")

    def check_results(self, api_caller) -> list[dict]:
        """
        Check pending accas for settled results using APICaller.get_fixture_result().

        Fixture results are fetched directly from API-Football since the local DB
        does not store final scores. Returns a list of newly settled accas.
        Updates win/loss records and removes settled accas from the pending list.
        """
        try:
            pending = self._get_pending()
            still_pending: list[dict] = []
            settled: list[dict] = []

            for acca in pending:
                all_settled = True

                for leg in acca.get("legs", []):
                    if leg.get("status") != "pending":
                        continue

                    result = api_caller.get_fixture_result(leg["fixture_id"])
                    if not result or not result.get("is_finished"):
                        all_settled = False
                        continue

                    home_g = result.get("home_goals")
                    away_g = result.get("away_goals")
                    if home_g is None or away_g is None:
                        all_settled = False
                        continue

                    market = leg.get("market", "")
                    pick = leg.get("pick", "")

                    if market == "1X2" and pick == "Home Win":
                        won = home_g > away_g
                    elif market == "1X2" and pick == "Away Win":
                        won = away_g > home_g
                    elif market == "BTTS Yes":
                        won = home_g > 0 and away_g > 0
                    elif market == "Over 2.5":
                        won = home_g + away_g > 2
                    elif market == "Double Chance" and pick == "1X":
                        won = home_g >= away_g
                    elif market == "Double Chance" and pick == "X2":
                        won = away_g >= home_g
                    elif market == "Asian Handicap":
                        won = home_g > away_g
                    else:
                        won = False

                    leg["status"] = "won" if won else "lost"

                if not all_settled:
                    still_pending.append(acca)
                    continue

                # All legs settled — determine acca outcome
                if all(l.get("status") == "won" for l in acca.get("legs", [])):
                    acca["acca_status"] = "won"
                else:
                    acca["acca_status"] = "lost"
                settled.append(acca)

                # Update record
                product = acca.get("product", "small_accas")
                record = self._get_record(product)
                if acca["acca_status"] == "won":
                    record["wins"] += 1
                    record["last_10"].append("W")
                else:
                    record["losses"] += 1
                    record["last_10"].append("L")
                record["last_10"] = record["last_10"][-10:]
                self._save_record(product, record)

            self._save_pending(still_pending)
            logger.info(
                "check_results: %d settled, %d still pending",
                len(settled), len(still_pending),
            )
            return settled

        except Exception:
            logger.exception("check_results failed")
            return []

    def get_compounding_signal(self, product: str) -> str:
        """
        Return a compounding signal string based on the last 10 results.

        Always appends a warning never to compound monster accas.
        """
        try:
            record = self._get_record(product)
            last_10 = record.get("last_10", [])
            if len(last_10) < 3:
                signal = "⚪ NOT ENOUGH DATA — track more first"
            else:
                rate = last_10.count("W") / len(last_10)
                if rate >= 0.80:
                    signal = "🟢 SAFE TO COMPOUND — strong run"
                elif rate >= 0.60:
                    signal = "🟡 COMPOUND CAUTIOUSLY — be conservative"
                elif rate >= 0.40:
                    signal = "🟠 CAUTION — mixed results, reduce stakes"
                else:
                    signal = "🔴 DO NOT COMPOUND — wait for form"
            return signal + "\n⚠️ Never compound 10k or 100k."
        except Exception:
            logger.exception(f"get_compounding_signal failed for product {product}")
            return "⚪ Signal unavailable.\n⚠️ Never compound 10k or 100k."

    def get_all_records(self) -> dict:
        """Return win/loss records for all products."""
        return {
            product: self._get_record(product)
            for product in [
                "small_accas", "best_acca",
                "acca_10k", "acca_100k",
                "acca_daily_100", "acca_daily_500", "acca_daily_1k",
            ]
        }

    def get_weekly_summary(self) -> dict:
        """Return a weekly performance summary dict for all products."""
        records = self.get_all_records()
        summary: dict[str, dict] = {}
        for product, record in records.items():
            total = record.get("wins", 0) + record.get("losses", 0)
            rate = (record["wins"] / total * 100) if total > 0 else 0.0
            summary[product] = {
                "wins": record.get("wins", 0),
                "losses": record.get("losses", 0),
                "win_rate": round(rate, 1),
                "signal": self.get_compounding_signal(product),
            }
        return summary
