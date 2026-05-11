"""
betslip/bot.py — Telegram bot for bet slip validation.

Uses python-telegram-bot (same library as the prediction bot) so it runs
identically in dev and production via run_polling() — no webhook needed.

Run with: python manage.py run_betslip_bot
"""
import logging

import requests
from django.conf import settings
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Synchronous helper — used by Celery tasks to reply to the user
# ---------------------------------------------------------------------------

def send_message(chat_id: int, text: str) -> None:
    """Fire-and-forget. Called from Celery workers (sync context)."""
    try:
        requests.post(
            f'https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage',
            json={'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'},
            timeout=10,
        )
    except Exception as exc:
        logger.error("send_message: failed — %s", exc)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "📊 BetSlip Validator\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "Send a photo of your bet slip and I'll validate\n"
                "each selection against FootieStat stats + Claude AI.\n\n"
                "Use /help to see all commands."
            ),
        )
    except Exception:
        logger.exception("/start handler failed")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "📊 BetSlip Validator\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "📷 Send a photo of your bet slip\n"
                "   → Extracts each bet via Claude Vision\n"
                "   → Matches to our fixture database\n"
                "   → Runs the same guards as the prediction engine\n"
                "   → Claude gives final ACCEPT / DOWNGRADE / REMOVE\n\n"
                "/start  — Welcome message\n"
                "/help   — This message"
            ),
        )
    except Exception:
        logger.exception("/help handler failed")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle photo messages — the main entry point."""
    from betslip.tasks import validate_bet_slip  # local import avoids circular  # noqa: PLC0415

    chat_id = update.effective_chat.id
    try:
        # Telegram sends multiple resolutions — take the largest
        photos = update.message.photo
        if not photos:
            return
        file_id = max(photos, key=lambda p: p.file_size).file_id

        await context.bot.send_message(
            chat_id=chat_id,
            text="⏳ Reading your bet slip...",
        )
        validate_bet_slip.delay(file_id, chat_id)

    except Exception:
        logger.exception("handle_photo failed")
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Something went wrong. Try sending the photo again.",
            )
        except Exception:
            pass


async def handle_non_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Catch-all for text and other message types."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📷 Send me a photo of your bet slip to validate it.",
        )
    except Exception:
        logger.exception("handle_non_photo failed")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_bot_app() -> Application:
    """Build and return a configured Telegram Application instance."""
    app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(~filters.PHOTO & ~filters.COMMAND, handle_non_photo))
    return app
