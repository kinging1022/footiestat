"""
telegram_bot.py — Telegram bot handlers for the prediction app.

All command handlers fire Celery tasks asynchronously and never await
the pipeline result directly. Each handler sends an immediate acknowledgement.
"""

import logging

from django.conf import settings
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from prediction.formatter import Formatter
from prediction.result_tracker import ResultTracker
from prediction.tasks import check_and_update_results, run_draw_pipeline, run_predict_pipeline

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Command handlers
# ------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start — introduce the bot."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "🤖 PredictionBot is live!\n"
                "Built on footiestat data + API-Football\n"
                "+ Claude AI validation.\n"
                "Use /help to see all commands."
            ),
        )
    except Exception:
        logger.exception("/start handler failed")


async def predict(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /predict — trigger the full daily pipeline."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ Generating all picks...",
        )
        run_predict_pipeline.delay("all")
    except Exception:
        logger.exception("/predict handler failed")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Failed to start pipeline. Check logs.",
            )
        except Exception:
            pass


async def best(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /best — trigger the best-acca-only pipeline."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ Finding best pick...",
        )
        run_predict_pipeline.delay("best")
    except Exception:
        logger.exception("/best handler failed")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Failed to start pipeline. Check logs.",
            )
        except Exception:
            pass


async def big(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /big — trigger the monster acca pipeline."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ Building monster accas (7-day window)...",
        )
        run_predict_pipeline.delay("big")
    except Exception:
        logger.exception("/big handler failed")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Failed to start pipeline. Check logs.",
            )
        except Exception:
            pass


async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /draw — daily and longshot draw picks."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ Finding draw picks...",
        )
        run_draw_pipeline.delay()
    except Exception:
        logger.exception("/draw handler failed")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Failed to start draw pipeline. Check logs.",
            )
        except Exception:
            pass


async def results(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /results — check and settle pending accas."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ Checking results...",
        )
        check_and_update_results.delay()
    except Exception:
        logger.exception("/results handler failed")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Failed to check results. Check logs.",
            )
        except Exception:
            pass


async def record(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /record — show win/loss record per product."""
    try:
        tracker = ResultTracker()
        formatter = Formatter()
        records = tracker.get_all_records()
        msg = formatter.format_record(records, tracker)
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text=msg
        )
    except Exception:
        logger.exception("/record handler failed")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Failed to fetch record.",
            )
        except Exception:
            pass


async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /weekly — show weekly performance summary."""
    try:
        tracker = ResultTracker()
        formatter = Formatter()
        summary = tracker.get_weekly_summary()
        msg = formatter.format_weekly_summary(summary)
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text=msg
        )
    except Exception:
        logger.exception("/weekly handler failed")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Failed to fetch weekly summary.",
            )
        except Exception:
            pass


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /status — bot health check."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "✅ PredictionBot running\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "Trigger: /predict /best /big\n"
                "Worker: footiestat Celery\n"
                "Cache: footiestat Redis\n"
                "DB: footiestat PostgreSQL\n"
                "API calls: /predictions + /odds only"
            ),
        )
    except Exception:
        logger.exception("/status handler failed")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help — show available commands."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "🤖 PredictionBot Commands\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "/predict — 10 daily accas + best + monsters\n"
                "/best    — Best acca only (today)\n"
                "/big     — 10k + 100k monster accas (7 days)\n"
                "/draw    — Daily + longshot draw picks (5–13)\n"
                "/results — Check and settle today's results\n"
                "/record  — Win/loss record per product\n"
                "/weekly  — Weekly performance summary\n"
                "/status  — Bot health check\n"
                "/help    — This message"
            ),
        )
    except Exception:
        logger.exception("/help handler failed")


# ------------------------------------------------------------------
# App factory
# ------------------------------------------------------------------

def create_bot_app() -> Application:
    """Build and return a configured Telegram Application instance."""
    app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("predict", predict))
    app.add_handler(CommandHandler("best", best))
    app.add_handler(CommandHandler("big", big))
    app.add_handler(CommandHandler("draw", draw))
    app.add_handler(CommandHandler("results", results))
    app.add_handler(CommandHandler("record", record))
    app.add_handler(CommandHandler("weekly", weekly))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("help", help_cmd))
    return app
