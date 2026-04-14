"""
run_bot.py — Management command to start the Telegram polling loop.

Usage: python manage.py run_bot
"""

import logging

from django.core.management.base import BaseCommand

from prediction.telegram_bot import create_bot_app

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Start the PredictionBot Telegram polling loop."""

    help = "Start the PredictionBot Telegram polling loop"

    def handle(self, *args, **kwargs) -> None:
        """Entry point — build the bot and start polling."""
        self.stdout.write("PredictionBot starting...")
        logger.info("PredictionBot starting")
        app = create_bot_app()
        app.run_polling(drop_pending_updates=True)
