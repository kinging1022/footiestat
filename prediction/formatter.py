"""
formatter.py — Plain-text message formatters for Telegram output.

All output is plain text (no MarkdownV2). Messages over 4000 chars
are split at newline boundaries. All methods return str or list[str].
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

MAX_MSG_LEN = 4000


class Formatter:
    """Formats prediction results as plain-text Telegram messages."""

    def split_message(self, text: str) -> list[str]:
        """
        Split a message into chunks of at most MAX_MSG_LEN characters.

        Splits at newline boundaries to avoid cutting words mid-line.
        """
        if len(text) <= MAX_MSG_LEN:
            return [text]

        chunks: list[str] = []
        current_lines: list[str] = []
        current_len = 0

        for line in text.split("\n"):
            line_len = len(line) + 1  # +1 for the newline
            if current_len + line_len > MAX_MSG_LEN and current_lines:
                chunks.append("\n".join(current_lines))
                current_lines = [line]
                current_len = line_len
            else:
                current_lines.append(line)
                current_len += line_len

        if current_lines:
            chunks.append("\n".join(current_lines))

        return chunks

    def format_header(self) -> str:
        """Return a bot header string with the current UTC date and time."""
        now = datetime.now(timezone.utc)
        return (
            "🤖 PredictionBot\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"📅 {now.strftime('%A %d %B %Y')}\n"
            f"⏰ {now.strftime('%H:%M')} UTC"
        )

    def format_small_acca(self, acca: dict, index: int) -> str:
        """Format a single daily accumulator bet as a plain-text message."""
        lines = [
            f"🟢 ACCA #{index} — Total Odds: {acca['total_odds']:.2f}",
            "━━━━━━━━━━━━━━━━━━",
        ]
        for i, leg in enumerate(acca.get("legs", []), 1):
            flag = "⭐" if leg.get("is_whitelisted") else "🔸"
            lines.append(
                f"{i}. {leg.get('home_team_name', '')} vs "
                f"{leg.get('away_team_name', '')}"
            )
            lines.append(
                f"   {leg.get('selected_market', '')} | "
                f"Odds: {leg.get('selected_odds', 0):.2f} | "
                f"Conf: {leg.get('adjusted_confidence', leg.get('confidence', 0))} {flag}"
            )
            lines.append(f"   Claude: {leg.get('claude_reason', '')}")
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(
            f"Avg Conf: {acca.get('avg_confidence', 0)} | "
            f"Legs: {acca.get('n_legs', len(acca.get('legs', [])))}"
        )
        return "\n".join(lines)

    def format_best_acca(self, acca: dict) -> str:
        """Format the best pick of the day accumulator."""
        lines = [
            f"⭐ BEST PICK OF THE DAY — Total Odds: {acca['total_odds']:.2f}",
            "━━━━━━━━━━━━━━━━━━",
        ]
        for i, leg in enumerate(acca.get("legs", []), 1):
            flag = "⭐" if leg.get("is_whitelisted") else "🔸"
            lines.append(
                f"{i}. {leg.get('home_team_name', '')} vs "
                f"{leg.get('away_team_name', '')}"
            )
            lines.append(
                f"   {leg.get('selected_market', '')} | "
                f"Odds: {leg.get('selected_odds', 0):.2f} | "
                f"Conf: {leg.get('adjusted_confidence', leg.get('confidence', 0))} {flag}"
            )
            lines.append(f"   Claude: {leg.get('claude_reason', '')}")
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(
            f"Avg Conf: {acca.get('avg_confidence', 0)} | "
            f"Legs: {acca.get('n_legs', len(acca.get('legs', [])))}"
        )
        return "\n".join(lines)

    def format_monster_acca(self, acca: dict, product: str) -> str:
        """Format a monster accumulator (10k or 100k) as a plain-text message."""
        emoji = "🔵" if product == "10k" else "🔴"
        header = f"{emoji} {product.upper()} ACCA"
        lines = [
            f"{header} — Total Odds: {acca.get('total_odds', 0):,.2f}",
            "━━━━━━━━━━━━━━━━━━",
        ]
        legs = acca.get("legs", [])
        for leg in legs[:5]:
            lines.append(
                f"[{leg.get('kickoff_date_short', '')}] "
                f"{leg.get('home_team_name', '')} vs "
                f"{leg.get('away_team_name', '')}"
            )
            lines.append(
                f"   {leg.get('selected_market', '')} | "
                f"Odds: {leg.get('selected_odds', 0):.2f} | "
                f"Conf: {leg.get('adjusted_confidence', leg.get('confidence', 0))}"
            )
        remaining = acca.get("n_legs", len(legs)) - 5
        if remaining > 0:
            lines.append(f"...and {remaining} more legs")
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(
            f"📅 {acca.get('start_date', '')} – {acca.get('end_date', '')}"
        )
        lines.append(
            f"🌍 {acca.get('n_leagues', 0)} leagues | "
            f"Avg Conf: {acca.get('avg_confidence', 0)}"
        )
        return "\n".join(lines)

    def format_insufficient(self, product: str) -> str:
        """Return a message for when there are not enough qualifying fixtures."""
        return (
            f"⚠️ Not enough qualifying fixtures\n"
            f"for {product}. Try again tomorrow."
        )

    def format_result_update(self, settled: list[dict]) -> str:
        """Format a result update message for settled accumulators."""
        if not settled:
            return "No settled accas found."
        lines = ["📊 Result Update", "━━━━━━━━━━━━━━━━━━"]
        for acca in settled:
            emoji = "✅" if acca.get("acca_status") == "won" else "❌"
            legs = acca.get("legs", [])
            won = sum(1 for l in legs if l.get("status") == "won")
            total = len(legs)
            lines.append(
                f"{emoji} {acca.get('product', '').upper()} — "
                f"{acca.get('acca_status', '').upper()} "
                f"({won}/{total} legs correct) | "
                f"Odds: {acca.get('total_odds', 0):.2f}"
            )
        return "\n".join(lines)

    def format_record(self, records: dict, tracker) -> str:
        """Format the win/loss record for all products."""
        lines = ["📈 Bot Record", "━━━━━━━━━━━━━━━━━━"]
        for product, record in records.items():
            total = record.get("wins", 0) + record.get("losses", 0)
            rate = (record["wins"] / total * 100) if total > 0 else 0.0
            signal = tracker.get_compounding_signal(product)
            lines.append(
                f"{product.upper()}: "
                f"{record.get('wins', 0)}W {record.get('losses', 0)}L "
                f"({rate:.0f}%)"
            )
            lines.append(f"  {signal}")
        return "\n".join(lines)

    def format_weekly_summary(self, summary: dict) -> str:
        """Format the weekly performance summary for all products."""
        lines = ["📈 Weekly Summary", "━━━━━━━━━━━━━━━━━━"]
        for product, data in summary.items():
            lines.append(
                f"{product.upper()}: "
                f"{data.get('wins', 0)}W {data.get('losses', 0)}L "
                f"({data.get('win_rate', 0)}%)"
            )
            lines.append(f"  {data.get('signal', '')}")
        return "\n".join(lines)
