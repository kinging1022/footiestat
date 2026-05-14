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
        # Sort by kickoff date ASC so start is always earliest, end always latest
        sorted_legs = sorted(legs, key=lambda l: l.get("date", ""))
        for i, leg in enumerate(sorted_legs, 1):
            lines.append(
                f"{i}. [{leg.get('kickoff_date_short', '')}] "
                f"{leg.get('home_team_name', '')} vs "
                f"{leg.get('away_team_name', '')}"
            )
            lines.append(
                f"   {leg.get('selected_market', '')} | "
                f"Odds: {leg.get('selected_odds', 0):.2f} | "
                f"Conf: {leg.get('adjusted_confidence', leg.get('confidence', 0))}"
            )
            if leg.get("claude_reason"):
                lines.append(f"   Claude: {leg.get('claude_reason')}")
        lines.append("━━━━━━━━━━━━━━━━━━")
        start_date = (
            sorted_legs[0].get("kickoff_date_short", "")
            if sorted_legs else acca.get("start_date", "")
        )
        end_date = (
            sorted_legs[-1].get("kickoff_date_short", "")
            if sorted_legs else acca.get("end_date", "")
        )
        lines.append(f"📅 {start_date} – {end_date}")
        lines.append(
            f"🌍 {acca.get('n_leagues', 0)} leagues | "
            f"Avg Conf: {acca.get('avg_confidence', 0)}"
        )
        return "\n".join(lines)

    def format_daily_monster_acca(self, acca: dict, product: str) -> str:
        """Format a daily monster acca (100x / 500x / 1k) as a plain-text message."""
        labels = {"100": "100X DAILY", "500": "500X DAILY", "1k": "1K DAILY"}
        emojis = {"100": "🟡", "500": "🟠", "1k": "🔴"}
        label = labels.get(product, f"{product.upper()} DAILY")
        emoji = emojis.get(product, "🔴")
        lines = [
            f"{emoji} {label} ACCA — Total Odds: {acca.get('total_odds', 0):,.2f}",
            "━━━━━━━━━━━━━━━━━━",
        ]
        for i, leg in enumerate(acca.get("legs", []), 1):
            lines.append(
                f"{i}. {leg.get('home_team_name', '')} vs "
                f"{leg.get('away_team_name', '')}"
            )
            lines.append(
                f"   {leg.get('selected_market', '')} | "
                f"Odds: {leg.get('selected_odds', 0):.2f} | "
                f"Conf: {leg.get('adjusted_confidence', leg.get('confidence', 0))}"
            )
            if leg.get("claude_reason"):
                lines.append(f"   Claude: {leg.get('claude_reason')}")
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(
            f"🌍 {acca.get('n_leagues', 0)} leagues | "
            f"Avg Conf: {acca.get('avg_confidence', 0)} | "
            f"Legs: {acca.get('n_legs', len(acca.get('legs', [])))}"
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

    def format_win_picks(self, picks: list[dict]) -> str:
        """Format up to 50 heavy-favourite win picks as a Telegram message."""
        if not picks:
            return (
                "🏆 Win Picks\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "No heavy-favourite win picks found today.\n"
                "Try again tomorrow."
            )

        lines = [
            "🏆 WIN PICKS  (odds ≤ 1.30)",
            "━━━━━━━━━━━━━━━━━━",
            f"Total: {len(picks)} picks",
            "",
        ]
        for i, pick in enumerate(picks, 1):
            side = pick.get("win_side", "")
            home = pick.get("home_team_name", "")
            away = pick.get("away_team_name", "")
            team = pick.get("win_team", "")
            label = "HOME WIN" if side == "home" else "AWAY WIN"
            lines.append(
                f"{i}. {home} vs {away}"
            )
            lines.append(
                f"   {label}: {team}  @{pick.get('win_odds', 0):.2f} "
                f"| API%: {pick.get('win_pct', 0):.0f}% "
                f"| Score: {pick.get('win_score', 0)}"
            )
            h2h_r = pick.get("h2h_win_rate")
            venue_r = pick.get("venue_win_rate")
            detail = f"   H2H: {h2h_r:.0%}" if h2h_r is not None else "   H2H: n/a"
            if venue_r is not None:
                detail += f" | Venue W%: {venue_r:.0%}"
            lines.append(detail)
            lines.append(f"   {pick.get('league_name', '')} — {pick.get('kickoff_str', '')}")

        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("⚠️ Heavy favourites — use as accumulator legs only.")
        return "\n".join(lines)

    def format_win_accas(self, result: dict) -> str:
        """Format 100x, 1K, and 100K win accumulators."""
        acca_100  = result.get("acca_100")
        acca_1k   = result.get("acca_1k")
        acca_100k = result.get("acca_100k")

        if result.get("insufficient") or not any([acca_100, acca_1k, acca_100k]):
            return (
                "🏆 Win Accas\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "Not enough qualifying win picks\n"
                "to build accumulators."
            )

        configs = [
            ("acca_100",  acca_100,  "⚡", "100x WIN ACCA"),
            ("acca_1k",   acca_1k,   "🔥", "1K WIN ACCA"),
            ("acca_100k", acca_100k, "💀", "100K WIN ACCA"),
        ]

        lines = ["🏆 WIN ACCUMULATORS", "━━━━━━━━━━━━━━━━━━"]
        first = True
        for _, acca, emoji, title in configs:
            if not acca:
                continue
            if not first:
                lines.append("")
            first = False
            lines.append(
                f"{emoji} {title} — Total Odds: {acca['total_odds']:,.2f}x"
            )
            lines.append("━━━━━━━━━━━━━━━━━━")
            sorted_legs = sorted(
                acca["legs"],
                key=lambda l: l.get("date", ""),
            )
            for i, leg in enumerate(sorted_legs, 1):
                side = leg.get("win_side", "")
                team = leg.get("win_team", "")
                label = "H" if side == "home" else "A"
                lines.append(
                    f"{i}. [{leg.get('kickoff_date_short', '')}] "
                    f"{leg.get('home_team_name', '')} vs "
                    f"{leg.get('away_team_name', '')}"
                )
                lines.append(
                    f"   [{label}] {team} Win  "
                    f"@{leg.get('win_odds', 0):.2f} | "
                    f"Score: {leg.get('win_score', 0)}"
                )
            start = acca.get("start_date") or (
                sorted_legs[0].get("kickoff_date_short", "") if sorted_legs else ""
            )
            end = acca.get("end_date") or (
                sorted_legs[-1].get("kickoff_date_short", "") if sorted_legs else ""
            )
            lines.append(f"📅 {start} – {end}")
            lines.append(
                f"🌍 {acca['n_leagues']} leagues | "
                f"Legs: {acca['n_legs']} | "
                f"Avg Score: {acca['avg_score']}"
            )

        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("⚠️ All legs ≤ 1.30 — high confidence, compound carefully.")
        return "\n".join(lines)
