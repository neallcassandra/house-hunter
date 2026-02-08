"""Telegram notifications. Formats property data and sends it."""

import logging
import os

from dotenv import load_dotenv
from openai import OpenAI
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .database import PropertyDatabase
from .state import PropertyData, ReviewResult

load_dotenv()
logger = logging.getLogger(__name__)


class SummarizerAgent:

    def __init__(self):
        self.bot_token = os.getenv("HOUSE_HUNTER_BOT_TOKEN")
        self.chat_id = os.getenv("HOUSE_HUNTER_CHAT_ID")

        if not self.bot_token or not self.chat_id:
            raise ValueError("HOUSE_HUNTER_BOT_TOKEN and HOUSE_HUNTER_CHAT_ID must be set")

        self.bot = Bot(token=self.bot_token)
        self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.db = PropertyDatabase()
        logger.info("SummarizerAgent initialized")

    async def summarize_and_notify(self, property_data: PropertyData, review_result: ReviewResult, force_notify: bool = False) -> bool:
        try:
            property_id = property_data["property_id"]

            if not force_notify and self.db.is_property_notified(property_id):
                logger.info(f"Property {property_id} already notified")
                return False

            message = self._format_telegram_message(property_data, review_result)
            success = await self._send_telegram_notification(message, property_data)

            if success:
                self.db.mark_property_notified(property_id, success=True)
                logger.info(f"✅ Notification sent for {property_id}")

            return success
        except Exception as e:
            logger.error(f"Error in summarize_and_notify: {e}")
            return False

    def _format_telegram_message(self, property_data: PropertyData, review_result: ReviewResult) -> str:
        state = os.getenv("HOUSE_HUNTER_STATE", "OH")
        lines = [
            "🏠 <b>NEW HOUSE FOUND!</b> 🏠\n",
            f"📍 <b>Address:</b> {property_data.get('address', 'Unknown')}",
            f"🏙️ <b>City:</b> {property_data.get('city', 'Unknown')}, {state}",
            f"💰 <b>Price:</b> ${property_data.get('price', 0):,}\n"
        ]

        details = []
        if property_data.get('beds'):
            details.append(f"🛏️ {property_data['beds']} bed")
        if property_data.get('baths'):
            details.append(f"🛁 {property_data['baths']} bath")
        if property_data.get('sqft'):
            details.append(f"📏 {property_data['sqft']:,} sqft")
        if property_data.get('year_built'):
            details.append(f"📅 Built {property_data['year_built']}")

        if details:
            lines.append(" | ".join(details) + "\n")

        # Market insights
        insights = self.db.get_market_insights(property_data)
        if insights:
            lines.append("<b>📊 Market Insights:</b>")

            if "days_on_market" in insights:
                days = insights["days_on_market"]
                if days == 0:
                    lines.append("  🆕 Just listed today!")
                elif days == 1:
                    lines.append("  🆕 Listed yesterday")
                elif days <= 7:
                    lines.append(f"  🔥 Listed {days} days ago (fresh!)")
                elif days <= 30:
                    lines.append(f"  📅 On market {days} days")
                elif days <= 60:
                    lines.append(f"  ⏰ On market {days} days (getting stale)")
                else:
                    lines.append(f"  ⚠️ On market {days} days (price negotiable?)")

            if "price_vs_avg_percent" in insights:
                diff_percent = insights["price_vs_avg_percent"]
                if diff_percent < -5:
                    lines.append(f"  💚 {abs(diff_percent):.0f}% below average for {property_data.get('city')}!")
                elif diff_percent < 5:
                    lines.append(f"  📊 Right at market average for {property_data.get('city')}")
                else:
                    lines.append(f"  📈 {diff_percent:.0f}% above average for {property_data.get('city')}")

            if "price_per_sqft_vs_avg" in insights:
                diff = insights["price_per_sqft_vs_avg"]
                prop_psf = insights["property_price_per_sqft"]
                if diff < 0:
                    lines.append(f"  💵 ${prop_psf}/sqft (great value!)")
                else:
                    lines.append(f"  💵 ${prop_psf}/sqft")

            lines.append("")

        lines.append("<b>✅ WHY IT PASSED:</b>")
        if property_data.get('basement_finished'):
            lines.append("  ✓ Finished basement")
        if not property_data.get('has_pool'):
            lines.append("  ✓ No pool")
        if property_data.get('price', 0) <= int(os.getenv("HOUSE_HUNTER_MAX_PRICE", "350000")):
            lines.append("  ✓ Within budget")

        return "\n".join(lines)

    async def _send_telegram_notification(self, message: str, property_data: PropertyData) -> bool:
        try:
            keyboard = None
            if property_data.get('listing_url'):
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 View Full Listing", url=property_data['listing_url'])]
                ])

            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
            return True
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    async def send_price_drop_notification(self, property_dict: dict) -> bool:
        try:
            state = os.getenv("HOUSE_HUNTER_STATE", "OH")
            lines = [
                "💰📉 <b>PRICE DROP ALERT!</b> 💰📉\n",
                f"📍 <b>Address:</b> {property_dict.get('address', 'Unknown')}",
                f"🏙️ <b>City:</b> {property_dict.get('city', 'Unknown')}, {state}\n",
                f"<b>Old Price:</b> <s>${property_dict.get('old_price', 0):,}</s>",
                f"<b>New Price:</b> ${property_dict.get('new_price', 0):,}",
                f"<b>💸 Savings:</b> ${property_dict.get('drop_amount', 0):,} ({property_dict.get('drop_percent', 0):.1f}% off!)\n"
            ]

            details = []
            if property_dict.get('beds'):
                details.append(f"🛏️ {property_dict['beds']} bed")
            if property_dict.get('baths'):
                details.append(f"🛁 {property_dict['baths']} bath")
            if property_dict.get('sqft'):
                details.append(f"📏 {property_dict['sqft']:,} sqft")

            if details:
                lines.append(" | ".join(details) + "\n")

            insights = self.db.get_market_insights(property_dict)
            if insights:
                lines.append("<b>📊 After Price Drop:</b>")

                if "days_on_market" in insights:
                    days = insights["days_on_market"]
                    if days <= 30:
                        lines.append(f"  📅 {days} days on market")
                    elif days <= 60:
                        lines.append(f"  ⏰ {days} days on market (motivated seller?)")
                    else:
                        lines.append(f"  ⚠️ {days} days on market (very motivated!)")

                if "price_vs_avg_percent" in insights:
                    diff_percent = insights["price_vs_avg_percent"]
                    if diff_percent < -5:
                        lines.append(f"  💚 Now {abs(diff_percent):.0f}% below market average!")
                    elif diff_percent < 5:
                        lines.append(f"  📊 Now at market average")
                    else:
                        lines.append(f"  📈 Still {diff_percent:.0f}% above average")

                if "property_price_per_sqft" in insights:
                    lines.append(f"  💵 Now ${insights['property_price_per_sqft']}/sqft")

            message = "\n".join(lines)

            keyboard = None
            if property_dict.get('listing_url'):
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 View Listing", url=property_dict['listing_url'])]
                ])

            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )

            self.db.mark_property_notified(property_dict.get('property_id'), success=True)
            logger.info(f"Sent price drop notification for {property_dict.get('property_id')}")
            return True

        except Exception as e:
            logger.error(f"Failed to send price drop notification: {e}")
            return False

    async def send_rejection_summary(self, total_found, reviewed_properties, api_calls_used, closest_match=None):
        """Send the "no matches" message with the closest miss."""
        try:
            lines = [
                "🏠💔 <b>No Perfect Matches Yet!</b>\n"
            ]

            if closest_match:
                closest_match_review = None
                for review in reviewed_properties:
                    if review["property_id"] == closest_match.get("property_id"):
                        closest_match_review = review
                        break

                lines.append("✨ <b>Closest match:</b>")
                lines.append(f"📍 {closest_match.get('address', 'Unknown')}")
                lines.append(f"💰 ${closest_match.get('price', 0):,}")

                if closest_match_review and closest_match_review.get("reasons"):
                    lines.append("\n<b>Issues with this one:</b>")
                    for reason in closest_match_review["reasons"]:
                        short_reason = reason[:50] + "..." if len(reason) > 50 else reason
                        lines.append(f"  ❌ {short_reason}")

            message = "\n".join(lines)

            keyboard = None
            if closest_match and closest_match.get('listing_url'):
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("👀 Check It Out Anyway", url=closest_match['listing_url'])]
                ])

            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )

            if closest_match:
                property_id = closest_match.get("property_id")
                if property_id:
                    self.db.mark_property_notified(property_id, success=True)
                    logger.info(f"✅ Marked closest match as notified: {property_id}")

            logger.info("Sent rejection summary")
            return True
        except Exception as e:
            logger.error(f"Failed to send rejection summary: {e}")
            return False

    async def send_error_notification(self, error_message: str, error_details: str = None) -> bool:
        try:
            lines = [
                "⚠️ <b>House Hunter Error</b> ⚠️\n",
                f"<b>Error:</b> {error_message}"
            ]

            if error_details:
                lines.append(f"\n<b>Details:</b>\n<code>{error_details[:500]}</code>")

            message = "\n".join(lines)

            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )

            logger.info("Sent error notification")
            return True

        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")
            return False

    async def send_weekly_summary(self) -> bool:
        try:
            stats = self.db.get_statistics()

            lines = [
                "📊 <b>Weekly House Hunter Summary</b> 📊\n",
                f"🏠 <b>Properties Checked:</b> {stats.get('last_7_days', 0)}",
                f"✅ <b>Passed Review:</b> {stats.get('properties_passed', 0)}",
                f"📬 <b>Notifications Sent:</b> {stats.get('properties_notified', 0)}",
            ]

            if stats.get('price_drops_last_7_days', 0) > 0:
                lines.append(f"💰 <b>Price Drops:</b> {stats['price_drops_last_7_days']}")

            by_city = stats.get('by_city', {})
            if by_city:
                lines.append("\n<b>By City:</b>")
                for city, count in sorted(by_city.items(), key=lambda x: x[1], reverse=True)[:5]:
                    lines.append(f"  • {city}: {count}")

            lines.append(f"\n<b>Total Properties Tracked:</b> {stats.get('total_properties', 0)}")

            message = "\n".join(lines)

            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )

            logger.info("Sent weekly summary")
            return True

        except Exception as e:
            logger.error(f"Failed to send weekly summary: {e}")
            return False
