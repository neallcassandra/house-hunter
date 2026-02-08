"""
Reviewer agent. Validates properties against search criteria using
a quick filter pass + GPT-4o-mini for the detailed stuff.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

from .state import PropertyData, ReviewResult

load_dotenv()
logger = logging.getLogger(__name__)


class ReviewerAgent:

    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # Load criteria from environment
        self.min_price = int(os.getenv("HOUSE_HUNTER_MIN_PRICE", "200000"))
        self.max_price = int(os.getenv("HOUSE_HUNTER_MAX_PRICE", "350000"))
        current_year = datetime.now().year
        self.min_year = current_year - 100

        # Cities to avoid
        avoid_str = os.getenv("HOUSE_HUNTER_AVOID_CITIES", "")
        self.avoid_cities = [c.strip() for c in avoid_str.split(",") if c.strip()]

        logger.info("ReviewerAgent initialized")

    def review_property(self, property_data: PropertyData) -> ReviewResult:
        """Run quick checks, then LLM review if it survives."""
        try:
            quick_check = self._quick_validation(property_data)
            if not quick_check["passes"]:
                return self._create_review_result(
                    property_data["property_id"],
                    passes=False,
                    reasons=quick_check["reasons"],
                    concerns=[],
                    missing_info=quick_check.get("missing_info", [])
                )

            return self._llm_review(property_data)

        except Exception as e:
            logger.error(f"Error reviewing property {property_data.get('property_id')}: {e}")
            return self._create_review_result(
                property_data.get("property_id"),
                passes=False,
                reasons=[f"Review failed: {str(e)}"],
                concerns=[],
                missing_info=[]
            )

    def _quick_validation(self, property_data: PropertyData) -> dict[str, Any]:
        """Price, location, age, type, pool - cheap checks before hitting the API."""
        reasons = []
        missing_info = []

        price = property_data.get("price")
        if price:
            if price < self.min_price:
                reasons.append(f"Price ${price:,} is below minimum ${self.min_price:,}")
            elif price > self.max_price:
                reasons.append(f"Price ${price:,} is above maximum ${self.max_price:,}")
        else:
            missing_info.append("Price information missing")

        city = property_data.get("city", "").strip()
        if city in self.avoid_cities:
            reasons.append(f"Located in {city} (excluded area)")

        year_built = property_data.get("year_built")
        if year_built and year_built < self.min_year:
            reasons.append("Over 100 years old")

        prop_type = property_data.get("property_type", "").lower()
        if prop_type and "single" not in prop_type and "house" not in prop_type:
            reasons.append(f"Property type is {prop_type} (not single-family)")

        if property_data.get("has_pool"):
            reasons.append("Has a pool (dealbreaker)")

        return {
            "passes": len(reasons) == 0,
            "reasons": reasons,
            "missing_info": missing_info
        }

    def _llm_review(self, property_data: PropertyData) -> ReviewResult:
        """
        Send property to GPT-4o-mini for detailed analysis. Mainly needed because
        basement data in the API is unreliable - sometimes it's in details, sometimes
        features, sometimes only the description text mentions it.
        """
        try:
            property_summary = self._format_property_for_llm(property_data)

            avoid_clause = ""
            if self.avoid_cities:
                avoid_clause = f"- Location in {' or '.join(self.avoid_cities)}\n"

            prompt = f"""You are a property reviewer helping someone find their perfect home.
Analyze this property against the following STRICT criteria:

MUST HAVE (all required):
- Price: ${self.min_price:,} - ${self.max_price:,}
- Year Built: {self.min_year} or newer (no century homes)
- Finished or Partially Finished basement (must have usable living space in basement - NOT unfinished/bare concrete)
  * Look for keywords: "finished basement", "partially finished", "renovated basement", "lower level living space", "walk-out basement", "daylight basement"
  * "Basement" alone without "finished" or "partial" context means REJECT
  * "Unfinished basement" or "rough basement" or "storage only" means REJECT
- Move-in ready condition (no major repairs or remodeling needed)

MUST NOT HAVE:
- Swimming pool
{avoid_clause}- Major structural issues or extensive repairs needed

Property Details:
{property_summary}

Analyze this property carefully. Consider:
1. Does it meet ALL the must-have criteria?
2. Are there any dealbreakers?
3. Is the basement clearly finished/partially finished, or is it ambiguous/unclear?
4. Any concerns even if it technically passes?

Respond in JSON format with SHORT reasons (5-10 words max):
{{
    "passes": true/false,
    "reasons": ["ONLY list SHORT reasons why it FAILS - leave empty if it passes"],
    "concerns": ["any concerns even if it passes"],
    "missing_info": ["important information that's missing"],
    "highlights": ["best features if it passes"],
    "basement_status": "finished|partial|unfinished|unclear|none"
}}
"""

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a thorough property analyst. Be strict about requirements."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3,
                max_tokens=500
            )

            llm_result = json.loads(response.choices[0].message.content)

            basement_status = llm_result.get("basement_status", "unknown")
            if basement_status:
                logger.info(f"LLM basement assessment for {property_data.get('address')}: {basement_status}")

            if not llm_result.get("passes", False) and basement_status in ["unclear", "none", "unfinished"]:
                if "basement" not in " ".join(llm_result.get("reasons", [])).lower():
                    llm_result.setdefault("reasons", []).append(f"Basement: {basement_status}")

            return self._create_review_result(
                property_data["property_id"],
                passes=llm_result.get("passes", False),
                reasons=llm_result.get("reasons", []),
                concerns=llm_result.get("concerns", []),
                missing_info=llm_result.get("missing_info", [])
            )

        except Exception as e:
            logger.error(f"LLM review failed: {e}")
            _ = self._quick_validation(property_data)
            return self._create_review_result(
                property_data["property_id"],
                passes=False,
                reasons=["LLM review failed, property rejected for safety"],
                concerns=[],
                missing_info=[]
            )

    def _format_property_for_llm(self, property_data: PropertyData) -> str:
        state = os.getenv("HOUSE_HUNTER_STATE", "OH")
        lines = []

        lines.append(f"Address: {property_data.get('address', 'Unknown')}")
        lines.append(f"City: {property_data.get('city', 'Unknown')}, {property_data.get('state', state)}")
        lines.append(f"Price: ${property_data.get('price', 0):,}")

        if property_data.get("beds"):
            lines.append(f"Bedrooms: {property_data['beds']}")
        if property_data.get("baths"):
            lines.append(f"Bathrooms: {property_data['baths']}")
        if property_data.get("sqft"):
            lines.append(f"Square Feet: {property_data['sqft']:,}")
        if property_data.get("year_built"):
            lines.append(f"Year Built: {property_data['year_built']}")
        if property_data.get("lot_size"):
            lines.append(f"Lot Size: {property_data['lot_size']:,} sq ft")

        lines.append(f"Property Type: {property_data.get('property_type', 'Unknown')}")

        if property_data.get("has_basement") is not None:
            if property_data["has_basement"]:
                if property_data.get("basement_finished"):
                    lines.append("Basement: Finished or Partial")
                else:
                    lines.append("Basement: Unfinished")
            else:
                lines.append("Basement: None")

        if property_data.get("has_pool") is not None:
            lines.append(f"Pool: {'Yes' if property_data['has_pool'] else 'No'}")

        if property_data.get("description"):
            lines.append(f"\nDescription: {property_data['description'][:500]}...")

        return "\n".join(lines)

    def _create_review_result(self, property_id, passes, reasons, concerns, missing_info):
        return {
            "property_id": property_id,
            "passes": passes,
            "reasons": reasons,
            "concerns": concerns,
            "missing_info": missing_info,
            "review_timestamp": datetime.now().isoformat()
        }

    def batch_review(self, properties: list[PropertyData]) -> list[ReviewResult]:
        results = []
        for i, prop in enumerate(properties, 1):
            logger.info(f"Reviewing property {i}/{len(properties)}: {prop.get('address')}")
            result = self.review_property(prop)
            results.append(result)

            if result["passes"]:
                logger.info(f" Property PASSED: {prop.get('address')}")
            else:
                logger.info(f"L Property FAILED: {', '.join(result['reasons'][:2])}")

        return results
