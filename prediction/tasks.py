"""
tasks.py — Celery tasks for the prediction pipeline.

Uses shared_task only. No new Celery app instance.
All pipeline state is cached in Redis. Telegram messages are split at 4000 chars.
"""

import json
import logging
import time

import redis
import requests
from celery import shared_task
from django.conf import settings

from prediction.api_caller import APICaller
from prediction.db_reader import DBReader
from prediction.engine import PredictionEngine
from prediction.formatter import Formatter
from prediction.result_tracker import ResultTracker
from prediction.validator import ClaudeValidator

logger = logging.getLogger(__name__)

REDIS_CLIENT = redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)

CACHE_KEYS = {
    "small":         "prediction:cache:small:accas",
    "daily_monster": "prediction:cache:daily:monster",
    "10k":           "prediction:cache:monster:10k",
    "100k":          "prediction:cache:monster:100k",
}


def send_telegram(message: str) -> None:
    """Send a plain-text message to Telegram. Splits if over 4000 chars."""
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        logger.warning("send_telegram: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    formatter = Formatter()
    chunks = formatter.split_message(message)

    for i, chunk in enumerate(chunks):
        try:
            requests.post(
                url,
                json={"chat_id": chat_id, "text": chunk},
                timeout=10,
            )
            if len(chunks) > 1 and i < len(chunks) - 1:
                time.sleep(0.5)
        except Exception as exc:
            logger.error(f"Telegram send failed: {exc}")


@shared_task(bind=True, max_retries=2)
def run_predict_pipeline(self, output_type: str = "all") -> None:
    """
    Main prediction pipeline task.

    output_type: 'all' | 'best' | 'big'
    Checks Redis cache before running each sub-pipeline.
    """
    db = DBReader()
    api = APICaller()
    engine = PredictionEngine()
    validator = ClaudeValidator()
    formatter = Formatter()
    tracker = ResultTracker()

    try:
        # ----------------------------------------------------------------
        # Small / daily accas
        # ----------------------------------------------------------------
        if output_type in ("all", "best"):

            cached_small = REDIS_CLIENT.get(CACHE_KEYS["small"])
            cached_dm    = REDIS_CLIENT.get(CACHE_KEYS["daily_monster"])

            if cached_small and cached_dm:
                logger.info("Serving small + daily monster accas from cache")
                small_result         = json.loads(cached_small)
                daily_monster_result = json.loads(cached_dm)
            else:
                logger.info("Running small + daily monster pipeline")
                start = time.time()

                fixtures = db.get_todays_fixtures("small")
                logger.info(f"Fixtures fetched: {len(fixtures)}")

                standings = db.get_batch_standings(fixtures)
                h2h_data = db.get_batch_h2h(fixtures)

                fixture_ids = [f["fixture_id"] for f in fixtures]
                predictions = api.get_batch_predictions(fixture_ids)
                odds = api.get_batch_odds(fixture_ids)

                eligible = engine.filter_eligible_fixtures(
                    fixtures, standings, predictions, odds, "small"
                )
                logger.info(f"Eligible fixtures: {len(eligible)}")

                scored = engine.score_all(
                    eligible, standings, h2h_data, predictions, odds, "small"
                )
                logger.info(f"Scored: {len(scored)}")

                validated = validator.validate_batch(scored)
                logger.info(f"Validated: {len(validated)}")

                small_result         = engine.build_accas(validated, "small")
                daily_monster_result = engine.build_accas(validated, "daily_monster")

                REDIS_CLIENT.setex(
                    CACHE_KEYS["small"],
                    settings.PREDICTION_CACHE_TTL_SMALL,
                    json.dumps(small_result, default=str),
                )
                REDIS_CLIENT.setex(
                    CACHE_KEYS["daily_monster"],
                    settings.PREDICTION_CACHE_TTL_SMALL,
                    json.dumps(daily_monster_result, default=str),
                )

                tracker.save_accas(small_result)
                tracker.save_accas(daily_monster_result)
                logger.info(
                    "Small + daily monster pipeline done in %.1fs", time.time() - start
                )

            # Send to Telegram
            send_telegram(formatter.format_header())

            if small_result.get("insufficient_fixtures"):
                send_telegram(formatter.format_insufficient("daily accas"))
            else:
                if output_type == "all":
                    for i, acca in enumerate(
                        small_result.get("daily_accas", []), 1
                    ):
                        send_telegram(formatter.format_small_acca(acca, i))

                if small_result.get("best_acca"):
                    send_telegram(
                        formatter.format_best_acca(small_result["best_acca"])
                    )

            # Daily monster — send whichever targets were built
            if daily_monster_result.get("insufficient_daily_monster_fixtures"):
                send_telegram(formatter.format_insufficient("daily monster accas"))
            else:
                for product, key in [("100", "acca_100"), ("500", "acca_500"), ("1k", "acca_1k")]:
                    acca = daily_monster_result.get(key)
                    if acca:
                        send_telegram(formatter.format_daily_monster_acca(acca, product))

        # ----------------------------------------------------------------
        # Monster accas
        # ----------------------------------------------------------------
        if output_type in ("big",):

            cached_10k = REDIS_CLIENT.get(CACHE_KEYS["10k"])
            cached_100k = REDIS_CLIENT.get(CACHE_KEYS["100k"])

            if cached_10k and cached_100k:
                logger.info("Serving monster accas from cache")
                acca_10k = json.loads(cached_10k)
                acca_100k = json.loads(cached_100k)
                monster_result = {
                    "acca_10k": acca_10k,
                    "acca_100k": acca_100k,
                    "insufficient_monster_fixtures": False,
                }
            else:
                logger.info("Running monster pipeline")
                start = time.time()

                m_fixtures = db.get_todays_fixtures("monster")
                logger.info(f"Monster fixtures: {len(m_fixtures)}")

                m_standings = db.get_batch_standings(m_fixtures)
                m_h2h = db.get_batch_h2h(m_fixtures)

                m_ids = [f["fixture_id"] for f in m_fixtures]
                m_preds = api.get_batch_predictions(m_ids)
                m_odds = api.get_batch_odds(m_ids)

                m_eligible = engine.filter_eligible_fixtures(
                    m_fixtures, m_standings, m_preds, m_odds, "monster"
                )

                m_scored = engine.score_all(
                    m_eligible, m_standings, m_h2h, m_preds, m_odds, "monster"
                )

                m_validated = validator.validate_batch(m_scored)

                monster_result = engine.build_accas(m_validated, "monster")

                if monster_result.get("acca_10k"):
                    REDIS_CLIENT.setex(
                        CACHE_KEYS["10k"],
                        settings.PREDICTION_CACHE_TTL_MONSTER,
                        json.dumps(monster_result["acca_10k"], default=str),
                    )
                if monster_result.get("acca_100k"):
                    REDIS_CLIENT.setex(
                        CACHE_KEYS["100k"],
                        settings.PREDICTION_CACHE_TTL_MONSTER,
                        json.dumps(monster_result["acca_100k"], default=str),
                    )

                tracker.save_accas(monster_result)
                logger.info(
                    "Monster pipeline done in %.1fs", time.time() - start
                )

            # Send header only if we didn't already send it above
            if output_type == "big":
                send_telegram(formatter.format_header())

            if monster_result.get("insufficient_monster_fixtures"):
                send_telegram(formatter.format_insufficient("monster accas"))
            else:
                if monster_result.get("acca_10k"):
                    send_telegram(
                        formatter.format_monster_acca(
                            monster_result["acca_10k"], "10k"
                        )
                    )
                if monster_result.get("acca_100k"):
                    send_telegram(
                        formatter.format_monster_acca(
                            monster_result["acca_100k"], "100k"
                        )
                    )

    except Exception as exc:
        logger.critical(f"run_predict_pipeline unhandled error: {exc}", exc_info=True)
        send_telegram("❌ Pipeline error. Check logs.")
        raise self.retry(exc=exc, countdown=60)


@shared_task
def check_and_update_results() -> None:
    """Check fixture results and settle any pending accas.

    Uses APICaller.get_fixture_result() — local DB does not store final scores.
    """
    api = APICaller()
    tracker = ResultTracker()
    formatter = Formatter()

    try:
        settled = tracker.check_results(api)
        if settled:
            msg = formatter.format_result_update(settled)
            send_telegram(msg)
        else:
            logger.info("No settled accas yet")
    except Exception:
        logger.exception("check_and_update_results failed")
