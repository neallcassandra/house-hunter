"""LangGraph workflow for the House Hunter system."""

import logging
import os
import uuid
from datetime import datetime
from typing import Any

from langgraph.graph import END, StateGraph
from langsmith import traceable

from .database import PropertyDatabase
from .reviewer import ReviewerAgent
from .scraper_agent import ScraperAgent
from .state import HouseHunterState
from .summarizer import SummarizerAgent

logger = logging.getLogger(__name__)


class HouseHunterGraph:
    """Wires up scraper → reviewer → summarizer as a LangGraph workflow."""

    def __init__(self):
        self.scraper = ScraperAgent()
        self.reviewer = ReviewerAgent()
        self.summarizer = SummarizerAgent()
        self.database = PropertyDatabase()

        # Clean up old entries on startup
        try:
            deleted = self.database.cleanup_old_entries(days_to_keep=90)
            if deleted > 0:
                logger.info(f"Database cleanup: removed {deleted} old entries")
        except Exception as e:
            logger.warning(f"Database cleanup failed: {e}")

        self.workflow = self._build_graph()
        self.app = self.workflow.compile()

        logger.info("HouseHunterGraph initialized")

    def _build_graph(self) -> StateGraph:
        workflow = StateGraph(HouseHunterState)

        workflow.add_node("scraper", self.scraper_node)
        workflow.add_node("reviewer", self.reviewer_node)
        workflow.add_node("summarizer", self.summarizer_node)

        workflow.set_entry_point("scraper")

        workflow.add_edge("scraper", "reviewer")
        workflow.add_edge("reviewer", "summarizer")
        workflow.add_edge("summarizer", END)

        return workflow

    def _calculate_property_score(self, prop: dict, review: dict) -> float:
        """
        Score rejected properties so we can find the "closest miss" to report.
        Starts at 100, deducts based on how bad each rejection reason is.
        """
        score = 100.0

        reasons = review.get("reasons", [])
        for reason in reasons:
            reason_lower = reason.lower()

            # Big dealbreakers
            if "pool" in reason_lower:
                score -= 50
            elif "cleveland" in reason_lower or "parma" in reason_lower:
                score -= 40
            elif "100 years old" in reason_lower or "century" in reason_lower:
                score -= 35
            elif "unfinished basement" in reason_lower:
                score -= 25
            elif "not single-family" in reason_lower:
                score -= 30
            elif "above maximum" in reason_lower:
                try:
                    price = prop.get("price", 0)
                    max_price = int(os.getenv("HOUSE_HUNTER_MAX_PRICE", "350000"))
                    if price > max_price:
                        overage = price - max_price
                        penalty = min(20, (overage / 10000) * 2)
                        score -= penalty
                except Exception:
                    score -= 15
            elif "below minimum" in reason_lower:
                score -= 10

        # Bonus for good stuff
        if prop.get("basement_finished"):
            score += 10
        if not prop.get("has_pool"):
            score += 5
        if prop.get("year_built", 0) >= 2000:
            score += 5
        if prop.get("sqft", 0) >= 1500:
            score += 3

        insights = self.database.get_market_insights(prop)
        if insights.get("price_vs_avg_percent", 0) < -5:
            score += 10

        return score

    @traceable(name="scraper_node")
    def scraper_node(self, state: HouseHunterState) -> HouseHunterState:
        try:
            logger.info("Starting scraper node")

            properties = self.scraper.search_properties()

            state["properties"] = properties
            state["api_calls_used"] = self.scraper.scraper.api_calls_made

            for prop in properties:
                self.database.mark_property_seen(prop)

            logger.info(f"Scraper found {len(properties)} properties")
            return state

        except Exception as e:
            logger.error(f"Scraper node error: {e}")
            state["errors"].append({"node": "scraper", "error": str(e)})
            return state

    @traceable(name="reviewer_node")
    def reviewer_node(self, state: HouseHunterState) -> HouseHunterState:
        try:
            logger.info("Starting reviewer node")

            properties = state.get("properties", [])
            reviewed = []
            passed = []

            for prop in properties:
                review_result = self.reviewer.review_property(prop)
                reviewed.append(review_result)

                if review_result["passes"]:
                    passed.append(prop)
                    logger.info(f"✅ Property passed: {prop.get('address')}")

                self.database.mark_property_seen(prop, review_result)

            state["reviewed_properties"] = reviewed
            state["passed_properties"] = passed

            logger.info(f"Reviewer: {len(passed)}/{len(properties)} properties passed")
            return state

        except Exception as e:
            logger.error(f"Reviewer node error: {e}")
            state["errors"].append({"node": "reviewer", "error": str(e)})
            return state

    @traceable(name="summarizer_node")
    async def summarizer_node(self, state: HouseHunterState) -> HouseHunterState:
        """Send notifications for matching properties, or the closest miss."""
        try:
            logger.info("Starting summarizer node")

            if not state.get("should_notify", True):
                logger.info("Notifications disabled, skipping")
                return state

            passed = state.get("passed_properties", [])
            reviewed = state.get("reviewed_properties", [])
            properties = state.get("properties", [])

            # Nothing passed, find and report the closest miss
            if len(passed) == 0 and len(properties) > 0:
                logger.info("No properties passed review, finding closest match")

                closest_match = None
                best_score = float('-inf')

                for review in reviewed:
                    if review["passes"]:
                        continue

                    prop = None
                    for p in properties:
                        if p["property_id"] == review["property_id"]:
                            prop = p
                            break

                    if not prop:
                        continue

                    if self.database.is_property_notified(prop["property_id"]):
                        logger.info(f"Skipping already notified property: {prop.get('address')}")
                        continue

                    score = self._calculate_property_score(prop, review)

                    if score > best_score:
                        closest_match = prop
                        best_score = score
                        logger.info(f"New closest match: {prop.get('address')} (score: {score:.2f})")

                if closest_match:
                    logger.info(f"Sending rejection summary with closest match: {closest_match.get('address')}")
                    await self.summarizer.send_rejection_summary(
                        total_found=len(properties),
                        reviewed_properties=reviewed,
                        api_calls_used=state.get("api_calls_used", 0),
                        closest_match=closest_match
                    )
                else:
                    logger.info("No new properties to report - skipping notification")

                state["notified_properties"] = []
                return state

            # Properties passed, notify about each one
            review_map = {r["property_id"]: r for r in reviewed}

            notified = []
            for prop in passed:
                review = review_map.get(prop["property_id"])
                if review:
                    success = await self.summarizer.summarize_and_notify(prop, review)
                    if success:
                        notified.append(prop["property_id"])

            state["notified_properties"] = notified
            logger.info(f"Sent {len(notified)} notifications")

            # Check for price drops
            if state.get("should_notify", True):
                logger.info("Checking for price drops...")
                price_drops = self.database.get_properties_with_price_drops(min_drop_percent=2.0)
                for prop_dict in price_drops:
                    if not self.database.is_property_notified(prop_dict.get("property_id")):
                        await self.summarizer.send_price_drop_notification(prop_dict)
                logger.info(f"Sent {len(price_drops)} price drop notifications")

            return state

        except Exception as e:
            logger.error(f"Summarizer node error: {e}")
            state["errors"].append({"node": "summarizer", "error": str(e)})
            return state

    async def run(self, test_mode: bool = False) -> dict[str, Any]:
        try:
            initial_state = {
                "run_id": str(uuid.uuid4()),
                "started_at": datetime.now().isoformat(),
                "completed_at": None,
                "min_price": int(os.getenv("HOUSE_HUNTER_MIN_PRICE", "200000")),
                "max_price": int(os.getenv("HOUSE_HUNTER_MAX_PRICE", "350000")),
                "cities": [],
                "properties": [],
                "reviewed_properties": [],
                "passed_properties": [],
                "notified_properties": [],
                "errors": [],
                "warnings": [],
                "api_calls_used": 0,
                "api_calls_limit": 20,
                "should_notify": not test_mode,
                "test_mode": test_mode
            }

            logger.info(f"Starting workflow run {initial_state['run_id']}")

            final_state = await self.app.ainvoke(initial_state)
            final_state["completed_at"] = datetime.now().isoformat()

            logger.info("Workflow completed:")
            logger.info(f"  - Properties found: {len(final_state.get('properties', []))}")
            logger.info(f"  - Properties passed: {len(final_state.get('passed_properties', []))}")
            logger.info(f"  - Notifications sent: {len(final_state.get('notified_properties', []))}")
            logger.info(f"  - API calls used: {final_state.get('api_calls_used', 0)}")

            errors = final_state.get("errors", [])
            if errors and not test_mode:
                error_summary = f"{len(errors)} error(s) occurred during workflow"
                error_details = "\n".join([f"{e.get('node', 'unknown')}: {e.get('error', 'unknown')}" for e in errors[:3]])
                await self.summarizer.send_error_notification(error_summary, error_details)

            return final_state

        except Exception as e:
            logger.error(f"Workflow error: {e}", exc_info=True)

            if not test_mode:
                try:
                    await self.summarizer.send_error_notification("Critical workflow failure", str(e))
                except Exception as notify_error:
                    logger.error(f"Failed to send error notification: {notify_error}")

            raise
