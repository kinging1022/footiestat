"""
management/commands/run_betslip_bot.py — Start the BetSlip Telegram bot.

Uses python-telegram-bot's run_polling() — same pattern as the prediction
bot's run_bot command.  Works identically in dev and production.
No webhook configuration needed.

Usage:
    python manage.py run_betslip_bot
"""
import logging

from django.core.management.base import BaseCommand

from betslip.bot import create_bot_app

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Start the BetSlip Telegram bot polling loop"

    def handle(self, *args, **kwargs) -> None:
        self.stdout.write("BetSlip bot starting...")
        logger.info("BetSlip bot starting")
        app = create_bot_app()
        app.run_polling(drop_pending_updates=True)
